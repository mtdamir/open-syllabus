import requests
import json
import pandas as pd
from mimesis import Generic
import multiprocessing as mp
from time import sleep


provider = Generic()


def get_json(url, params=None):
    resp = requests.get(
        url, params=params, headers={"User-Agent": provider.internet.user_agent()}
    )
    data = json.loads(resp.text)
    return data

# sbf means syllabiByField
def get_work_sbf(work_id):
    data = get_json(
        url=f"https://explorer-api.opensyllabus.org/v1/works/{work_id}.json"
    )
    return {"syllabiByField": data["syllabiByField"]}


def get_works_url():
    fields = get_json(
        url="https://explorer-api.opensyllabus.org/v1/fields.json", params={"size": 62}
    )["results"]["fields"]
    for i, field in enumerate(fields):
        print(f"{i}. getting {field['id']} urls")
        size = 1000
        data = get_json(
            url="https://explorer-api.opensyllabus.org/v1/works.json",
            params={"size": size, "fields": field["id"]},
        )["results"]["works"]
        df = pd.DataFrame(data)
        df["field"] = [field["id"]] * size
        df["url"] = df["id"].apply(
            lambda x: f"https://explorer-api.opensyllabus.org/v1/works/{x}.json"
        )
        df = df.loc[:, ["field", "url"]]
        df.to_csv("urls.csv", mode="a", index=False)


def get_work(url, field):
    # id,rank,score,name,subtitle,appearances,openAccess,publishDate,syllabiByField,publisher,persons,field
    data = get_json(url=url)
    data_series = pd.Series({**data, "field": field})
    cols = [
        "id",
        "rank",
        "score",
        "name",
        "subtitle",
        "appearances",
        "openAccess",
        "publishDate",
        "syllabiByField",
        "publisher",
        "persons",
        "field",
    ]

    return data_series.to_dict()


def get_multi_work(start,end):
    df = pd.read_csv('urls.csv').iloc[start:end,]
    for label, series in df.iterrows():
       yield get_work(series['url'],series['field'])



def save_multi_work(start,end):
    df = pd.DataFrame(data=get_multi_work(start,end))
    df.to_csv('Data.csv',mode='a')
    
save_multi_work(0,1000)

    


# print(
#     get_work(
#         "https://explorer-api.opensyllabus.org/v1/works/8589935103080.json", "Hebrew"
#     )
# )
