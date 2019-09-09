"""
Using the drug_concept_id, one can infer the values to populate the route concept ID field
pseudoephedrine hydrochloride 7.5 MG Chewable Tablet (OMOP: 43012486) would have route as oral
"""


def get_route_mapping_queries(project_id, dataset_id):
    queries = []
    return queries


if __name__ == '__main__':
    import cdr_cleaner.args_parser as parser
    import cdr_cleaner.clean_cdr_engine as clean_engine

    ARGS = parser.parse_args()
    clean_engine.add_console_logging(ARGS.console_log)
    query_list = get_route_mapping_queries(ARGS.project_id, ARGS.dataset_id)
    clean_engine.clean_dataset(ARGS.project_id, ARGS.dataset_id, query_list)
