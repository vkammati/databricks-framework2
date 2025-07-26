"""
Collibra Integration Module
"""
import requests
import json
import logging
from pyspark.sql.functions import regexp_replace, split, array_join, col, initcap, explode, coalesce, lit, \
    concat, concat_ws, length, collect_list
from datta_pipeline_library.core.spark_init import spark
from datta_pipeline_library.core.utils import has_column


def retrieve_table(inter_dict, tbl_name):
    """
    Takes an input dictionary and returns the ['TableName', 'TableDisplayName', 'TableContainsColumn']
    keys for a particular input table name
    :param inter_dict: List of dictonary for multiple table with keys
    ['LifDataProductName', 'LifDataProductBusinessName', 'LifDataProductType',
    'LifDataProductDataConfidentiality', 'LifDataProductStorageLocation', 'LifDataProductContainsTable']

    :param tbl_name: input table name to filter.
    :return: list of dictionary containing keys ['TableName', 'TableDisplayName', 'TableContainsColumn']
    """
    value = ""
    status = "Accepted"
    for x in inter_dict['SchemaContainsTable']:
        if x['Table'][0]['TableDisplayName'] == tbl_name and \
                x['Table'][0]['TableStatus'][0]['TableStatusName'] == status:
            value = x['Table'][0]
    return value


def fetch_metadata_json(tbl_name, user, pwd, api_url, json_string):
    """
    function takes table name as input and return a dictionary of enriched metadata from EDC
    :param tbl_name: table name as input
    :param user: Collibra User id
    :param pwd: Collibra Password
    :param api_url: url information of Collibra Api
    :param json_string: json string payload when calling the Collibra REST API
    :return: dictionary of enriched metadata for the particular table ['ColumnName', 'ColumnDataConfidentiality',
    'ColumnPersonalData', 'ColumnDataAggregationRisk', 'ConfidentialTechnicalInformation']

    """

    response = requests.post(api_url, auth=(user, pwd),
                             data=json.dumps(json_string))
    print("************* For using UTF-8 *******************")
    response.encoding = response.apparent_encoding
    print("*************************************************")
    result = json.loads(response.text.replace(u"\u201c", "'").replace(u"\u201d", "'"))

    # filtering only accepted widget for specific name
    inter_dict = result['view']['Domain'][0]['Schema'][0]

    # Filtering only specific table
    output_dict = retrieve_table(inter_dict, tbl_name)['TableContainsColumn']

    return output_dict


def fetch_metadata_dataframe(tbl_name, user, pwd, api_url, json_string):
    """
    Flattens the json output to dataframe

    @param tbl_name: Table Name
    @param user: User
    @param pwd: Password
    @param api_url: Collibra API URL
    @param json_string: Collibra Input Json String
    @return: Dataframe
    """
    df = spark.createDataFrame(fetch_metadata_json(tbl_name,user,pwd, api_url, json_string))
    mdf = df.select(explode("column"))
    keys = (mdf
            .select(explode("col"))
            .select("key")
            .distinct()
            .rdd.flatMap(lambda x: x)
            .collect())

    exprs = [col("col").getItem(k).alias(k) for k in keys]
    return mdf.select(*exprs)


def create_schema_xfrm(df):
    """
    Dataframe containing column_type, nullability,alias_name,column_name to generate schema StructType
    :param df: Dataframe
    :return: Dataframe
    """
    if has_column(df, "SkipColumn"):
        df = df.withColumn("SkipColumn",
                           regexp_replace(regexp_replace(split(col("SkipColumn"), "=")[1], "}]", ""), " ", "_"))
    else:
        df = df.withColumn("SkipColumn", lit("false"))

    if has_column(df, "ColumnTargetName"):
        df = df.withColumn("ColumnTargetName", regexp_replace(split(col("ColumnTargetName"), "=")[1], "}]", ""))
    else:
        df = df.withColumn("ColumnTargetName", col("ColumnDisplayName"))

    return (df.select("ColumnDisplayName",
                      coalesce("ColumnTargetName", "ColumnDisplayName").alias("ColumnTargetName"),
                      coalesce("SkipColumn", lit("false")).alias("SkipColumn")))


def schema_expr(df):
    """
    Takes schema dataframe and returns select expression

    :param df: Dataframe
    :return: Select expression
    """
    return (df
    .where("SkipColumn = 'false'")
    .select(collect_list(concat("ColumnDisplayName", lit(" as "), "ColumnTargetName")))
    .rdd.map(lambda x: x[0]).collect()[0])


def concat_col(prefix, y):
    """
    contacts each column value with target column by replacing the CV prefix.
    For example `ColumnName` has value `state` which is prefix here
    and `ColumnTags` has value  `CV.ConfidentialTechnicalInformation` after the udf runs
    output: `state.ConfidentialTechnicalInformation` as target column value.
    :param prefix: prefix string
    :param y: input string
    :return: replaces Static prefix CV with prefix String
    """
    if y is None:
        return y
    else:
        return y.replace("CV", prefix)


def clmn_expr(clmn_name):
    """
    Takes the column value removes special characters like `[]` or `{}`
    and replaces `ColumnTagValue=` with `CV.`
    :param clmn_name:
    :return: column expression
    """

    if clmn_name.lower() == "columntags":
        return array_join((split(
            regexp_replace(regexp_replace(col("ColumnTags"), "ColumnTagValue=", ".ColumnTag:"), "[\[\]{}]", ""),
            ",")), ",")
    else:
        return regexp_replace(concat(col("ColumnDisplayName"), lit(f".{clmn_name}:"),
                      initcap(regexp_replace(split(col(clmn_name), "=")[1], "}]", ""))), ",", "")


def create_table_tags(df):
    """
    Fucntion takes a DataFrame has input and combines various attributes as final Table Tags.

    :param df: Input Dataframe which takes tag values and flatten them store in
    corresponding columns with column name as prefix for the tags
    :return: DataFrame
    #concat_col_udf = udf(concat_col, StringType())
    """
    # [TODO]: Improve this function when there are multiple tag values `ColumnTags`

    return (df.withColumn("ColumnDataAggregationRisk", clmn_expr("ColumnDataAggregationRisk"))
            .withColumn("ColumnDescription", clmn_expr("ColumnDescription"))
            .withColumn("ColumnCommerciallySensitiveInformation", clmn_expr("ColumnCommerciallySensitiveInformation"))
            .withColumn("ColumnSensitivePersonalData", clmn_expr("ColumnSensitivePersonalData"))
            .withColumn("ConfidentialTechnicalInformation", clmn_expr("ConfidentialTechnicalInformation"))
            .withColumn("ColumnDataConfidentiality", clmn_expr("ColumnDataConfidentiality"))
            .withColumn("ColumnDisplayName", clmn_expr("ColumnDisplayName"))
            .withColumn("ColumnPersonalData", clmn_expr("ColumnPersonalData"))
            .withColumn("ColumnSoxScope", clmn_expr("ColumnSoxScope"))
            .select("ColumnDisplayName", "ColumnDataAggregationRisk", "ColumnDescription",
                    "ColumnCommerciallySensitiveInformation", "ConfidentialTechnicalInformation",
                    "ColumnDataConfidentiality", "ColumnPersonalData", "ColumnSoxScope"))


def flatten(l):
    """
    Flatten a list of list into a single list.

    :param l: A list of list
    :return: A Single List containing all the elements of the input lists
    """
    # [TODO]Better error handling of single list etc.
    return [item for sublist in l for item in sublist]


def convert(lst):
    """
    Convert a list to a dictionary that maps elements at even indices to elements at the following odd indices
     as key value pairs
    :param lst:  A list of elements
    :return: dict: A dictionary that maps the elements at even indices of `lst` to the
    elements at the following odd indices.
    """
    res_dct = {lst[i]: lst[i + 1] for i in range(0, len(lst), 2)}
    return res_dct


def get_tags(df):
    """
    Get a dictionary of tags from a input dataframe
    :param df: A DataFrame with `tags` column
    :return: A dictionary of tags as key value pair with tag name and tag value.
    """
    tagList = df.select("tags").rdd.flatMap(lambda x: x).collect()
    joinedList = ",".join(tagList)
    kv = [x.split(":") for x in joinedList.split(",")]
    return convert(flatten(kv))


def fetch_business_metadata(tbl_name, collibra_config):
    """
    Reads metadata from Collibra and splits into schema as exprs and tags
    @param tbl_name: Table Name
    @param collibra_config: Collibra Config
    @return:
    """
    user = collibra_config.edc_user_id
    pwd = collibra_config.edc_user_pwd
    api_url = collibra_config.api_url
    json_string = collibra_config.json_string

    exprs = "*"
    tags = ""
    spark.conf.set("spark.sql.caseSensitive", "false")

    try:
        """ Api Call to Dataframe """
        df = fetch_metadata_dataframe(tbl_name, user, pwd, api_url, json_string)

        print("edc/collibra.py fetch_business_metadata() function df::", df)
        schema = create_schema_xfrm(df)
        exprs = schema_expr(schema)
        # Tags
        tagsDF = (create_table_tags(df)
                  .select("ColumnDisplayName", concat_ws(",", "ColumnDisplayName", "ColumnDataAggregationRisk",
                                                         "ColumnDescription", "ColumnCommerciallySensitiveInformation",
                                                         "ConfidentialTechnicalInformation",
                                                         "ColumnDataConfidentiality",
                                                         "ColumnPersonalData", "ColumnSoxScope").alias("tags"))
                  .withColumn("len", length("tags")).filter("len <> 0").drop("len")
                  .distinct()
                  )
        
        print("edc/collibra.py fetch_business_metadata() function tagsDF::", tagsDF)
        tags = get_tags(tagsDF)
        print("edc/collibra.py fetch_business_metadata() function tags::", tags)

    except Exception as e:
        print(str(e))
        logging.warning("EDC Metadata Fetch returned empty or Errored Out")
        print("EDC Metadata Fetch returned empty or Errored Out")

    spark.conf.set("spark.sql.caseSensitive", "true")
    return exprs, tags
