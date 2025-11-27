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

## パート 0: ブートストラップファイル
# このプロジェクトを開始する際に、最初に実行する必要があります。
# 要件（requirements）をインストールし、`STORAGE` および `STORAGE_MODE` の
# 環境変数を作成します。そして、該当する場合、`raw/WA_Fn-UseC_-Telco-Customer-Churn-.csv`
# にあるデータを `STORAGE` の指定されたパスにコピーします。

# `STORAGE` 環境変数は、DataLake が Hive データを保存するために使用する
# クラウドストレージの場所です。AWS の場合は `s3a://〜〜`、Azure の場合は
# `abfs://〜〜`、Cloudera AI クラスター上の場合は `hdfs://〜〜` となります。

# 依存ライブラリをインストールします
!pip3 install -r requirements.txt

# ライブラリのインポート
from cmlbootstrap import CMLBootstrap
from IPython.display import Javascript, HTML
import os
import time
import json
import requests
import xml.etree.ElementTree as ET
import datetime
import subprocess

run_time_suffix = datetime.datetime.now()
run_time_suffix = run_time_suffix.strftime("%d%m%Y%H%M%S")

# CMLBootstrap で必要となるセットアップ変数を設定します
HOST = os.getenv("CDSW_API_URL").split(":")[0] + "://" + os.getenv("CDSW_DOMAIN")
USERNAME = os.getenv("CDSW_PROJECT_URL").split("/")[6]  # args.username  # "vdibia"
API_KEY = os.getenv("CDSW_API_KEY")
PROJECT_NAME = os.getenv("CDSW_PROJECT")

# API ラッパーをインスタンス化します
cml = CMLBootstrap(HOST, USERNAME, API_KEY, PROJECT_NAME)

# STORAGE 環境変数を設定します
try:
    storage = os.environ["STORAGE"]
except:
    # デフォルトの外部ストレージの場所を設定します
    storage = "/user/" + os.getenv("HADOOP_USER_NAME") 
    
    # 利用可能な場合、PbC のための外部ストレージの場所を設定します
    if os.path.exists("/etc/hadoop/conf/hive-site.xml"):
        tree = ET.parse("/etc/hadoop/conf/hive-site.xml")
        root = tree.getroot()
        for prop in root.findall("property"):
            if prop.find("name").text == "hive.metastore.warehouse.dir":
                # 誤った PVC 外部ストレージのロケールを捕捉します
                if len(prop.find("value").text.split("/")) > 5:
                    storage = (
                        prop.find("value").text.split("/")[0]
                        + "//"
                        + prop.find("value").text.split("/")[2]
                    )

    # storage 環境変数を作成し設定します
    storage_environment = cml.create_environment_variable({"STORAGE": storage})
    os.environ["STORAGE"] = storage
    
  
# HDFS 上でコマンドを実行するための関数を定義します
def run_cmd(cmd, raise_err=True):

    """
    Python の subprocess モジュールを使用して Linux コマンドを実行します。
    引数:
        cmd (str) - 実行する Linux コマンド
    戻り値:
        process オブジェクト
    """
    print("システムコマンドを実行中: {0}".format(cmd))

    proc = subprocess.run(
        cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT
    )

    if proc.returncode != 0 and raise_err == True:
        raise RuntimeError(
            "コマンド実行エラー: {}. リターンコード: {}, 出力: {}, エラー: {}".format(
                cmd, proc.returncode, proc.stdout, proc.stderr
            )
        )

    return proc


# クラウドストレージへのデータアップロードを試行し、エラーが発生した場合は、
# プロジェクト構築にローカルストレージを使用することを示す環境変数を設定します
try:
    dataset_check = run_cmd(
        f'hdfs dfs -test -f {os.environ["STORAGE"]}/{os.environ["DATA_LOCATION"]}/WA_Fn-UseC_-Telco-Customer-Churn-.csv',
        raise_err=False,
    )

    if dataset_check.returncode != 0:
        run_cmd(
            f'hdfs dfs -mkdir -p {os.environ["STORAGE"]}/{os.environ["DATA_LOCATION"]}'
        )
        run_cmd(
            f'hdfs dfs -copyFromLocal /home/cdsw/raw/WA_Fn-UseC_-Telco-Customer-Churn-.csv {os.environ["STORAGE"]}/{os.environ["DATA_LOCATION"]}/WA_Fn-UseC_-Telco-Customer-Churn-.csv'
        )
    cml.create_environment_variable({"STORAGE_MODE": "external"})
except RuntimeError as error:
    cml.create_environment_variable({"STORAGE_MODE": "local"})
    print(
        "外部データストアと対話できなかったため、ローカルプロジェクトストレージが使用されます。HDFS DFS コマンドは以下のエラーで失敗しました:"
    )
    print(error)

# モデルの系統（リネージ）追跡のための YAML ファイルを作成します
# ドキュメント: https://docs.cloudera.com/machine-learning/cloud/model-governance/topics/ml-registering-lineage-for-model.html
yaml_text = f"""Churn Model API Endpoint:
        hive_table_qualified_names:                                             # これは、トレーニングデータにリンクするための事前定義されたキーです
            - "{os.environ["HIVE_DATABASE"]}.{os.environ["HIVE_TABLE"]}@cm"     # Hive テーブルオブジェクトを表す qualifiedName です
        metadata:                                                               # これは、追加のメタデータのための事前定義されたキーです
            query: "select * from historical_data"                              # 推奨される使用例: トレーニングデータを抽出するために使用されたクエリ
            training_file: "code/4_train_models.py"                             # 推奨される使用例: 使用されたトレーニングファイル
    """

with open("lineage.yml", "w") as lineage:
    lineage.write(yaml_text)
