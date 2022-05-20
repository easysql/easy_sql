from easy_sql.sql_processor.funcs_rdb import  ModelFuncs
ModelFuncs(None)\
    .bq_model_predict_with_tmp_spark("/Users/qinke.yang/Documents/workbench/dataplat-gcp-demo"
                                  "/workflow/sales/dm/label/model/demo/v1","ods_sales.sales_model_demo_with_label",
                                  "ods_sales.sales_model_demo_with_label_out",
                                  "*","id","label")