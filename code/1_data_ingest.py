# ###########################################################################
#
#  CLOUDERA APPLIED MACHINE LEARNING PROTOTYPE (AMP)
#  (C) Cloudera, Inc. 2021
#  All rights reserved.
#
#  適用されるオープンソースライセンス: Apache 2.0
#
#  注記: Cloudera のオープンソース製品は、個々に著作権が設定された数百の
#  コンポーネントで構成されるモジュール式のソフトウェア製品です。
#  各 Cloudera のオープンソース製品は、米国著作権法における集合著作物です。
#  集合著作物を使用するためのライセンスは、Cloudera との書面による契約に
#  基づいて提供されます。集合著作物から切り離して使用する場合、このファイルは
#  上記で特定されたオープンソースライセンスに従って使用が許諾されます。
#
#  このコードは、(i) Cloudera, Inc. または (ii) このコードを配布することを
#  許可された第三者との書面による契約に基づいてお客様に提供されます。
#  Cloudera または権限を持ち適切にライセンスされた第三者との書面による契約が
#  ない場合、お客様にはこのコードにアクセスまたは使用する権利はありません。
#
#  Cloudera, Inc.（「Cloudera」）との間で書面による別段の合意がない限り、
#  A) CLOUDERA は、いかなる種類の保証もなくこのコードをお客様に提供します。
#  (B) CLOUDERA は、権原、非侵害、商品性、特定の目的に対する適合性の黙示的な
#  保証を含むがこれらに限定されない、このコードに関する一切の明示的および
#  黙示的な保証を否認します。
#  (C) CLOUDERA は、このコードから生じるまたは関連するいかなる請求についても、
#  お客様に対して責任を負わず、防御、補償、または免責することはありません。
#  (D) お客様に付与されたこのコードに対する権利の行使に関して、CLOUDERA は、
#  逸失収益、逸失利益、所得の喪失、事業上の利益または利用不能の喪失、
#  またはデータの喪失や破損に関連する損害を含むがこれらに限定されない、
#  直接的、間接的、付随的、特別、懲罰的、または結果的な損害について一切責任を
#  負いません。
#
# ###########################################################################

# パート 1: データ取り込み (データ収集)
# データサイエンティストは、自分の環境に様々なデータを取り込む必要があります。
# それをサポートするため、Cloudera AI は多くのソースからデータを取り込むことができます。
# データが .csv ファイル、parquet や feather のようなモダンな形式、クラウドストレージ、
# または SQL データベースのどこにあっても、CML はデータサイエンティストにとって
# 使いやすい環境でそれらを扱えるようにします。

# ご自身のコンピューター上のローカルデータへのアクセス
#
# ご自身のコンピューターに保存されているデータにアクセスするには、[ファイルを Cloudera AIの ファイルシステムに
# アップロードし、そこから参照する](https://docs.cloudera.com/machine-learning/cloud/import-data/topics/ml-accessing-local-data-from-your-computer.html)という手順を踏みます。
#
# > プロジェクトの**概要 (Overview)** ページに移動します。**ファイル (Files)** セクションの下にある
# > **アップロード (Upload)** をクリックし、関連するデータファイルを選択してアップロード先フォルダを指定します。
#
# 例えば、`mydata.csv` というファイルを `data` というフォルダにアップロードした場合、
# 次のサンプルコードが機能します。

# ```
# import pandas as pd
#
# df = pd.read_csv('data/mydata.csv')
#
# # または:
# df = pd.read_csv('/home/cdsw/data/mydata.csv')
# ```

# S3 のデータへのアクセス
#
# [Amazon S3 のデータ](https://docs.cloudera.com/machine-learning/cloud/import-data/topics/ml-accessing-data-in-amazon-s3-buckets.html)
# へのアクセスは、Cloudera AI ファイルシステムへの取得と保存という、上述の手順に従います。
# > Amazon Web Services のアクセスキーを、プロジェクトの
# > [環境変数](https://docs.cloudera.com/machine-learning/cloud/import-data/topics/ml-environment-variables.html)
# > として `AWS_ACCESS_KEY_ID` と `AWS_SECRET_ACCESS_KEY` に追加します。
#
# CDP DataLake で使用されるアクセスキーを取得するには、
# [この Cloudera Community チュートリアル](https://community.cloudera.com/t5/Community-Articles/How-to-get-AWS-access-keys-via-IDBroker-in-CDP/ta-p/295485)を参照してください。

#
# 次のサンプルコードは、S3 バケット `data_bucket` から `myfile.csv` というファイルを
# 取得し、Cloudera AI のホームフォルダに保存します。
# ```
# # Boto S3 接続オブジェクトを作成します。
# from boto.s3.connection import S3Connection
# aws_connection = S3Connection()
#
# # データセットをファイル 'myfile.csv' にダウンロードします。
# bucket = aws_connection.get_bucket('data_bucket')
# key = bucket.get_key('myfile.csv')
# key.get_contents_to_filename('/home/cdsw/myfile.csv')
# ```


# クラウドストレージまたは Hive メタストアからのデータアクセス
#
# CML に付属する [Hive メタストア](https://docs.cloudera.com/machine-learning/cloud/import-data/topics/ml-accessing-data-from-apache-hive.html)
# からのデータアクセスは、あといくつかの手順が必要です。
# まず、クラウドストレージからデータを取得し、それを Hive テーブルとして保存する必要があります。
#
# > 最初に、プロジェクト設定で `STORAGE` を
# > [環境変数](https://docs.cloudera.com/machine-learning/cloud/import-data/topics/ml-environment-variables.html)
# > として指定します。これには、DataLake が Hive データを保存するために使用する
# > クラウドストレージの場所を含めます。AWS では `s3a://〜〜``、Azure では
# > `abfs://〜〜`、オンプレミスの CDSW クラスターでは `hdfs://〜〜`` となります。
#
# これは、`0_bootstrap.py` を実行した際にすでに設定されていますので、次のコードは
# そのまま実行できるように構成されています。
# コードはインポートと `SparkSession` の作成から始まります。

import os
import sys
import subprocess

from cmlbootstrap import CMLBootstrap
from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException
from pyspark.sql.types import *

# CMLBootstrap で必要となるセットアップ変数を設定します
HOST = os.getenv("CDSW_API_URL").split(":")[0] + "://" + os.getenv("CDSW_DOMAIN")
USERNAME = os.getenv("CDSW_PROJECT_URL").split("/")[6]  # args.username  # "vdibia"
API_KEY = os.getenv("CDSW_API_KEY")
PROJECT_NAME = os.getenv("CDSW_PROJECT")

# API ラッパーをインスタンス化します
cml = CMLBootstrap(HOST, USERNAME, API_KEY, PROJECT_NAME)

# STORAGE 環境変数を設定します
# TODO: このコードブロックは 0_bootstrap.py から繰り返されているため、修正が必要です
try:
    storage = os.environ["STORAGE"]
except:
    # デフォルトの外部ストレージの場所を設定します
    storage = "/user/" + os.getenv("HADOOP_USER_NAME") 
    
    # 利用可能な Hive テーブルがあるかを確認します
    if os.path.exists("/etc/hadoop/conf/hive-site.xml"):
        tree = ET.parse("/etc/hadoop/conf/hive-site.xml")
        root = tree.getroot()
        for prop in root.findall("property"):
            if prop.find("name").text == "hive.metastore.warehouse.dir":
                # 外部ストレージのロケールを調整します
                if len(prop.find("value").text.split("/")) > 5:
                    storage = (
                        prop.find("value").text.split("/")[0]
                        + "//"
                        + prop.find("value").text.split("/")[2]
                    )

    # storage 環境変数を作成し設定します
    storage_environment = cml.create_environment_variable({"STORAGE": storage})
    os.environ["STORAGE"] = storage
    


spark = SparkSession.builder.appName("PythonSQL").master("local[*]").getOrCreate()

# **注記:**
# ファイルサイズが大きくないため、Spark ローカルモードでの実行で問題ありませんが、
# Kubernetes クラスター上で Spark を実行したい場合は、次の設定を追加できます。
#
# > .config("spark.yarn.access.hadoopFileSystems",os.getenv['STORAGE'])\
#
# そして `.master("local[*]")\` を削除します。
#

# データの構造がすでにわかっているので、事前にスキーマを追加できます。
# Spark はスキーマを推論することもできますが、その場合、データ**全体**を読み取ってしまうため、事前にスキーマを追加しておくことは良い習慣です。
schema = StructType(
    [
        StructField("customerID", StringType(), True),
        StructField("gender", StringType(), True),
        StructField("SeniorCitizen", StringType(), True),
        StructField("Partner", StringType(), True),
        StructField("Dependents", StringType(), True),
        StructField("tenure", DoubleType(), True),
        StructField("PhoneService", StringType(), True),
        StructField("MultipleLines", StringType(), True),
        StructField("InternetService", StringType(), True),
        StructField("OnlineSecurity", StringType(), True),
        StructField("OnlineBackup", StringType(), True),
        StructField("DeviceProtection", StringType(), True),
        StructField("TechSupport", StringType(), True),
        StructField("StreamingTV", StringType(), True),
        StructField("StreamingMovies", StringType(), True),
        StructField("Contract", StringType(), True),
        StructField("PaperlessBilling", StringType(), True),
        StructField("PaymentMethod", StringType(), True),
        StructField("MonthlyCharges", DoubleType(), True),
        StructField("TotalCharges", DoubleType(), True),
        StructField("Churn", StringType(), True),
    ]
)

# これでデータを Spark に読み込むことができます
storage = os.environ["STORAGE"]
data_location = os.environ["DATA_LOCATION"]
hive_database = os.environ["HIVE_DATABASE"]
hive_table = os.environ["HIVE_TABLE"]
hive_table_fq = hive_database + "." + hive_table

if os.environ["STORAGE_MODE"] == "external":
    path = f"{storage}/{data_location}/WA_Fn-UseC_-Telco-Customer-Churn-.csv"
else:
    path = "/home/cdsw/raw/WA_Fn-UseC_-Telco-Customer-Churn-.csv"

if os.environ["STORAGE_MODE"] == "external":
    
    try:
        telco_data = spark.read.csv(path, header=True, schema=schema, sep=",", nullValue="NA")

        # ...そしてデータを検査します。
        telco_data.show()

        telco_data.printSchema()

        # これで、Spark DataFrame をローカルの Cloudera AI のファイルシステムにファイルとして
        # *および* (可能であれば) プロジェクトの他の部分で使用される Hive のテーブルとして保存できます。
        telco_data.coalesce(1).write.csv(
            "file:/home/cdsw/raw/telco-data/", mode="overwrite", header=True
        )
        spark.sql("show databases").show()
        spark.sql("show tables in " + hive_database).show()

        # 可能であれば Hive テーブルを作成します
        # プロジェクトの他の部分で使用される Hive 内のテーブルが
        # まだ存在しない場合は、作成するという処理です。
        if hive_table not in list(
            spark.sql("show tables in " + hive_database).toPandas()["tableName"]
        ):
            print(hive_table + " テーブルを作成しています")

            try:
                telco_data.write.format("parquet").mode("overwrite").saveAsTable(
                    hive_table_fq
                )
            except AnalysisException as ae:
                print(ae)
                print("ストレージロケーションから競合するディレクトリを削除しています。")

                conflict_location = f'{os.environ["STORAGE"]}/datalake/data/warehouse/tablespace/external/hive/{os.environ["HIVE_TABLE"]}'
                cmd = ["hdfs", "dfs", "-rm", "-r", conflict_location]
                subprocess.call(cmd)
                telco_data.write.format("parquet").mode("overwrite").saveAsTable(
                    hive_table_fq
                )

            # Hive テーブル内のデータを表示します
            spark.sql("select * from " + hive_table_fq).show()

            # Hive テーブルに関するより詳細な情報を取得するには、これを実行できます:
            spark.sql("describe formatted " + hive_table_fq).toPandas()

    except Exception as e:
        print(e)
        print("STORAGE_MODE == local で AMP を続行します")
        cml.create_environment_variable({"STORAGE_MODE": "local"})


# その他のデータアクセス方法

# 他の場所からデータにアクセスする方法については、
# [Cloudera AI ドキュメント](https://docs.cloudera.com/machine-learning/cloud/import-data/index.html)を参照してください。

# スケジュール済みジョブ (Scheduled Jobs)
#
# Cloudera AI の機能の 1 つに、cron ジョブに似た、定期的な間隔でコードの実行をスケジュールする
# 機能があります。これは、**データパイプライン**、**ETL**、**定期レポート作成**などの
# ユースケースに役立ちます。新しいデータファイルが定期的に（例：毎時ログファイルなど）
# 作成される場合、上記のコードを含むデータローディングスクリプトを実行する
# ジョブをスケジュールできます。

# > 任意のスクリプトを [ジョブとしてスケジュール](https://docs.cloudera.com/machine-learning/cloud/jobs-pipelines/topics/ml-creating-a-job.html)できます。
# > 特定のコマンドライン引数や環境変数を持つジョブを作成できます。
# > ジョブは他のジョブの完了によってトリガーされ、[パイプライン](https://docs.cloudera.com/machine-learning/cloud/jobs-pipelines/topics/ml-creating-a-pipeline.html)
# > を形成できます。スクリプトが `/home/cdsw/job1/output.csv` に保存する
# > csv レポートなどの添付ファイル付きで、個人にメールを送信するようにジョブを設定できます。

# このあとは、このスクリプト `1_data_ingest.py` をジョブとして登録してみましょう。
