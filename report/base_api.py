from flask.ext.restful import Resource

class BaseApi(Resource):
    """Base class for API handlers.

    Provides a generic implementation for an API handler which is backed by a
    BaseDao and supports POST and GET.

    For APIs that support PUT requests as well, extend from UpdatableApi instead.

    When extending this class, prefer to use the method_decorators class property
    for uniform authentication, e.g.:
      method_decorators = [api_util.auth_required_cron]
    """
    def __init__(self, dao, get_returns_children=False):
        self.dao = dao
        self._get_returns_children = get_returns_children

    def get(self, id_=None, participant_id=None):
        """Handle a GET request.

        Args:
          id: If provided this is the id of the object to fetch.  If this is not
            present, this is assumed to be a "list" request, and the list() function
            will be called.
        """
        if id_ is None:
            return self.list(participant_id)
        obj = self.dao.get_with_children(id_) if self._get_returns_children else self.dao.get(id_)
        if not obj:
            raise NotFound("%s with ID %s not found" % (self.dao.model_type.__name__, id_))
        if participant_id:
            if participant_id != obj.participantId:
                raise NotFound("%s with ID %s is not for participant with ID %s" %
                               (self.dao.model_type.__name__, id_, participant_id))
        return self._make_response(obj)

    def _make_response(self, obj):
        return self.dao.to_client_json(obj)

    def _get_model_to_insert(self, resource, participant_id=None):
        # Children of participants accept a participant_id parameter to from_client_json; others don't.
        if participant_id is not None:
            return self.dao.from_client_json(
                resource, participant_id=participant_id, client_id=api_util.get_oauth_id())
        else:
            return self.dao.from_client_json(resource, client_id=api_util.get_oauth_id())

    def _do_insert(self, m):
        self.dao.insert(m)

    def post(self, participant_id=None):
        """Handles a POST (insert) request.

        Args:
          participant_id: The ancestor id.
        """
        resource = request.get_json(force=True)
        m = self._get_model_to_insert(resource, participant_id)
        self._do_insert(m)
        return self._make_response(m)

    def list(self, participant_id=None):
        """Handles a list request, as the default behavior when a GET has no id provided.

        Subclasses should pull the query parameters from the request with
        request.args.get().
        """
        #pylint: disable=unused-argument
        raise BadRequest('List not implemented, provide GET with an ID.')

    def _query(self, id_field, participant_id=None):
        """Run a query against the DAO.
        Extracts query parameters from request using FHIR conventions.
        Returns an FHIR Bundle containing entries for each item in the
        results, with a "next" link if there are more results to fetch. An empty Bundle
        will be returned if no results match the query.
        Args:
          id_field: name of the field containing the ID used when constructing resource URLs for results
          participant_id: the participant ID under which to perform this query, if appropriate
        """
        logging.info('Preparing query for %s.', self.dao.model_type)
        query = self._make_query()
        results = self.dao.query(query)
        logging.info('Query complete, bundling results.')
        response = self._make_bundle(results, id_field, participant_id)
        logging.info('Returning response.')
        return response

    def _make_query(self):
        field_filters = []
        max_results = DEFAULT_MAX_RESULTS
        pagination_token = None
        order_by = None
        for key, value in request.args.iteritems(multi=True):
            if key == '_count':
                max_results = int(request.args['_count'])
                if max_results < 1:
                    raise BadRequest("_count < 1")
                if max_results > MAX_MAX_RESULTS:
                    raise BadRequest("_count exceeds {}".format(MAX_MAX_RESULTS))
            elif key == '_token':
                pagination_token = value
            elif key == '_sort' or key == '_sort:asc':
                order_by = OrderBy(value, True)
            elif key == '_sort:desc':
                order_by = OrderBy(value, False)
            else:
                field_filter = self.dao.make_query_filter(key, value)
                if field_filter:
                    field_filters.append(field_filter)
        return Query(field_filters, order_by, max_results, pagination_token)

    def _make_bundle(self, results, id_field, participant_id):
        import main
        bundle_dict = {"resourceType": "Bundle", "type": "searchset"}
        if results.pagination_token:
            query_params = request.args.copy()
            query_params['_token'] = results.pagination_token
            next_url = main.api.url_for(self.__class__, _external=True, **query_params)
            bundle_dict['link'] = [{"relation": "next", "url": next_url}]
        entries = []
        for item in results.items:
            json = self.dao.to_client_json(item)
            if participant_id:
                full_url = main.api.url_for(self.__class__,
                                            id_=json[id_field],
                                            p_id=to_client_participant_id(participant_id),
                                            _external=True)
            else:
                full_url = main.api.url_for(self.__class__,
                                            p_id=json[id_field],
                                            _external=True)
            entries.append({"fullUrl": full_url,
                            "resource": json})
        bundle_dict['entry'] = entries
        return bundle_dict

