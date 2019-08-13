# AOU - De-identification Engine

This engine will run de-identificataion rules againsts a given table, certain rules are applied to all tables (if possible)
We have divided rules and application of the rules, in order to have granular visibility into what is happening

DESIGN:

    - The solution :
    The Application of most of the rules are handled in the SQL projection, this allows for simpler jobs with no risk of limitations around joins imposed by big-query.
    By dissecting operations in this manner it is possible to log the nature of an operation on a given attribute as well as pull some sample data to illustrate it

    We defined a vocabulary of rule specifications :
        -fields         Attributes upon which a given rule can be applied
        -values         conditional values that determine an out-come of a rule (can be followed by an operation like REGEXP)
                            If followed by "apply":"REGEXP" the the values are assumed to be applied using regular expressions
                            If NOT followed by anything the values are assumed to be integral values and and the IN operator is used instead
        -into           outcome related to a rule
        -key_field      attribute to be used as a filter that can be specified by value_field or values
        -value_field    value associated with a key_field
        -on             suggests a meta table and will have filter condition when generalization or a field name for row based suppression


   Overall there are rules that suggest what needs to happen on values, and there is a file specifying how to apply the rule on a given table.

    - The constraints:

        1. Bigquery is designed to be used as a warehouse not an RDBMS.That being said
            a. it lends itself to uncontrollable information redundancies and proliferation.
            b. The use of relational concepts such as foreign keys are pointless in addition to the fact that there is not referential integrity support in bigquery.
            As a result thre is no mechanism that guarantees data-integrity.


        2. We have a method "simulate" that acts as a sampler to provide some visibility into what this engine has done given an attribute and the value space of the data.
        This potentially adds to data redundancies.  It must remain internal.

LIMITATIONS:

    - The engine is not able to validate the rules without having to submit the job i.e it's only when the rubber hits the road that we know!
    Also that's the point of submitting a job

    - The engine can not simulate complex cases, its intent is to help by providing information about basic scenarios.

    - The engine does not resolve issues of consistency with data for instance : if a record has M,F on two fields for gender ... this issue is out of the scope of deid.
    Because it relates to data-integrity.

NOTES:

    There is an undocumented feature enabled via a hack i.e Clustering a table. The API (SDK) does NOT provide a clean way to perform clustering a table.

    To try to compensate for this, the developed approach looks for redundancies using regular expressions and other information available.
    Also developed a means by which identifiers can be used in future iterations.

USAGE :

    python aou.py --rules <path.json> --idataset <name> --private_key <file> --table <table.json> --action [submit,simulate|debug] [--cluster] [--log <path>]

    --rule  will point to the JSON file contianing rules
        --idataset  name of the input dataset (an output dataset with suffix _deid will be generated)
        --table     path of that specify how rules are to be applied on a table
        --private_key   service account file location
        --action        what to do:
                        simulate    will generate simulation without creating an output table
                        submit      will create an output table
                        debug       will just print output without simulation or submit (runs alone)
        --cluster     This flag enables clustering on person_id
