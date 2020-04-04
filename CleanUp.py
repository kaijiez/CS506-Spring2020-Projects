from fuzzywuzzy import fuzz
from fuzzywuzzy import process
import os
import re
from operator import itemgetter
import pandas as pd
import numpy as np

#read data in batches,
def readfile(filename):
    # entries=os.listdir("data/")
    streets = []
    ownerNames=[]

    df = pd.read_csv(os.path.join("data/",filename))

    x = pd.DataFrame(df.owner_addr)
    y = pd.DataFrame(df.owner_name)
    z = pd.DataFrame(df.owner_city)
    q = pd.DataFrame(df.owner_stat)
    result = pd.concat([x,z,q,y], axis=1, join='inner')
    result = result.values.tolist()
    for row in result:
        streets.append(str(row[0])+" "+str(row[1])+" "+str(row[2]))
        ownerNames.append(str(row[3]))

    # print(list(zip(streets, ownerNames)))
    df["FullOwnerAddress"] = pd.DataFrame(streets)
    df["FullOwnerAddress"].fillna("",inplace=True)
    df["owner_name"] = pd.DataFrame(ownerNames)
    df["owner_name"].fillna("",inplace=True)
    print(df["FullOwnerAddress"].head)
    print(df.shape)
    return df

def matchAgencyNames(filename,data):
    # print(data.columns.values)
    df = pd.read_csv(os.path.join("data/",filename),encoding = "ISO-8859-1")
    agency = pd.DataFrame(df.Agency)
    address = pd.DataFrame(df.Address)
    result = pd.concat([agency,address],axis=1,join='inner')
    result = result.values.tolist()
    # res={"Name":[],"Address":[]}

    choice=agency.values.tolist()

    #match names with agencynames if score>50 otherwise keep original names
    data['owner_name'].apply(lambda x: max([(fuzz.ratio(x,i),x) for i in choice],key=itemgetter(0))[1][0] if max([(fuzz.ratio(x,i),x) for i in choice],key=itemgetter(0))[0]>50 else x )

    print("printing")
    data.to_csv("./result/MatchWithAgencyNames.csv", index=False)

    #very long runtime, need optimization
    return data



def compareOwnerNames(data):
    tuples = pd.concat([data["FullOwnerAddress"],data["owner_name"]],axis=1).values.tolist()
    curr=""
    # choice=[tuples[0][1]]
    backtrack=0
    prevaddr=tuples[0][0]
    for tup in range(1,len(tuples)):
        # build up choice list
        # if(tuples[tup][0]==prevaddr):
        #     choice.append(tuples[tup][1])
        if(tuples[tup][0]!=prevaddr or tup==(len(tuples)-1)):
            # find owner name with highest score
            scores={}
            for i in range(backtrack,tup):
                if(tuples[i][1] not in scores):
                    scores[tuples[i][1]]=0
                else:
                    similarity=[(fuzz.ratio(tuples[i][1], x), x) for x in scores.keys()]
                    for score,name in similarity:
                        scores[name]+=score

                    #add all scores for all names in "score" for fairness
                    # if(process.extractOne(tuples[i][1],choice)!=None):
                    #     scores[tuples[i][1]]+=process.extractOne(tuples[i][1],choice)[1]

                    # [(fuzz.ratio(tuples[i][1],x),x) for x in choice]

            standardizeName=max(scores, key=scores.get)
            # print("standardize name",scores.keys())

            #standardize owner names in data
            if(max(scores.values())>0):
                for i in range(backtrack,tup):
                    tuples[i]=(tuples[i][0],standardizeName)

            #reset variables
            prevaddr=tuples[tup][0]
            backtrack=tup
            choice=[]

    print("comparing owner names")
    # print(data["FullOwnerAddress"].head())
    # print(data["objectid"].head())
    df = pd.DataFrame(tuples,columns=['FullOwnerAddress',"owner_name"])
    df.reset_index(drop=True, inplace=True)
    data.reset_index(drop=True, inplace=True)
    df = pd.concat([df, data["objectid"]],axis=1)
    # print(df["FullOwnerAddress"].head())
    # print("data",data["objectid"].head())
    # print("df", df['objectid'].head())
    result = data.merge(df, left_on="objectid", right_on="objectid")
    # print(result["FullOwnerAddress"].head())
    # print(result["objectid"].head())
    print("df",df.shape)
    print("data",data.shape)
    return data



def sort_streets(data):

    data["FullOwnerAddress"] = data["FullOwnerAddress"].apply(lambda x: cleanupStreet(x))
    data.sort_values(by=['FullOwnerAddress'],inplace=True)
    print(data.shape)
    return data

def cleanupStreet(street):
    regex = re.compile(
        r'(?P<prefix>^North\w*\s|^South\w*\s|^East\w*\s|^West\w*\s|^N\.?\s|^S\.?\s|^E\.?\s|^W\.?\s)?(?P<street>.*)',
        re.IGNORECASE
    )
    street_prefix=""
    street = street.strip()
    street_match = regex.search(street)
    if street_match.group('prefix'):
            street_prefix = street_match.group('prefix')

    street_root = street_match.group('street')
    return street_prefix +street_root

def extractStreetTuple(street):
    return (street[0][1],street[0][0])

def mergeFile(filename):
    dataset1=pd.read_csv(os.path.join("data/",filename))
    dataset2=pd.read_csv("./result/MatchWithAgencyNames.csv")
    print(dataset1.objectid.head())
    dataset1.objectid=dataset1.objectid.astype(np.int64)
    # dataset2=dataset2.astype(object)
    # print("dataset1",dataset1.dtypes)
    result=dataset1.merge(dataset2,left_on="objectid",right_on="objectid")
    print(result.head())


def main():
    # data=readfile("original_luc_gt_909.csv")
    # print("finished reading data")

    # streets=sort_streets(data)
    # print("finished sorted street")

    # data=compareOwnerNames(streets)
    # print("finished comparing owner names")

    # matchAgencyNames("MassGovernmentAgencyList.csv",data)

    mergeFile("std_name.csv")

main()
