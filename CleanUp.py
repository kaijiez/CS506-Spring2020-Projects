from fuzzywuzzy import fuzz
from fuzzywuzzy import process
import os
import re
from operator import itemgetter
import pandas as pd

#read data in batches,
def readfile(filename):
    # entries=os.listdir("data/")
    streets = []
    ownerNames=[]

    df = pd.read_csv(os.path.join("data/",filename),encoding = "ISO-8859-1")

    x = pd.DataFrame(df.owner_addr)
    y = pd.DataFrame(df.owner_name)
    z = pd.DataFrame(df.owner_city)
    q = pd.DataFrame(df.owner_stat)
    result = pd.concat([x,z,q,y], axis=1, join='inner')
    result = result.values[:1000].tolist()
    for row in result:
        streets.append(str(row[0])+" "+str(row[1])+" "+str(row[2]))
        ownerNames.append(str(row[3]))

    print(list(zip(streets, ownerNames)))
    return list(zip(streets, ownerNames))

def matchAgencyNames(filename,data):
    df = pd.read_csv(os.path.join("data/",filename),encoding = "ISO-8859-1")
    agency = pd.DataFrame(df.Agency)
    address = pd.DataFrame(df.Address)
    result = pd.concat([agency,address],axis=1,join='inner')
    result = result.values.tolist()
    res={"Name":[],"Address":[]}

    choice=agency.values.tolist()
    for tuple in range(len(data)):
        ls=[(fuzz.ratio(data[tuple][1], x), x) for x in choice] #ls=[(score,name)]

        #use Name from Agency list if socre >50, else keep original Name
        if (max(ls, key=itemgetter(0))[0]>50):
            print("found one")
            res["Name"].append(max(ls, key=itemgetter(0))[1][0])
            res["Address"].append(data[tuple][0])
        else:
            res["Name"].append(data[tuple][1])
            res["Address"].append(data[tuple][0])

    print("printing")
    df_r = pd.DataFrame(res,columns=["Name","Address"])
    df_r.to_csv("./result/CleanONResult.csv")

    #very long runtime, need optimization
    return res



def compareOwnerNames(tuples):
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


    return tuples



def sort_streets(street_list):
    """
    Sort streets alphabetically, ignoring cardinal direction prefixes such as North, South, East and West
    :param street_list: list of street names
    """
    # compile a sorted list to extract the direction prefixes and street root from the street string
    # created using https://regex101.com/#python
    regex = re.compile(
        r'(?P<prefix>^North\w*\s|^South\w*\s|^East\w*\s|^West\w*\s|^N\.?\s|^S\.?\s|^E\.?\s|^W\.?\s)?(?P<street>.*)',
        re.IGNORECASE
    )

    # list to store tuples for sorting
    street_sort_list = []

    # for every street
    counter=0
    for street in street_list:
        street_prefix=""
        # just in case, strip leading and trailing whitespace
        street = street[0].strip()

        # extract the prefix and street using regular expression matching
        street_match = regex.search(street)

        # convert both the returned strings to lowercase
        if street_match.group('prefix'):
            street_prefix = street_match.group('prefix')

        street_root = street_match.group('street')

        # place the prefix, street extract and full street string in a tuple and add it to the list
        street_sort_list.append(((street_prefix, street_root, street),list(map(itemgetter(1),street_list))[counter]))
        counter+=1

    # sort the streets first on street name, and second with the prefix to address duplicates
    street_sort_list.sort(key=extractStreetTuple, reverse=False)

    # return just a list of sorted streets, using a list comprehension to pull out the original street name in order

    return [(street_tuple[0][2],street_tuple[1]) for street_tuple in street_sort_list]

def extractStreetTuple(street):
    return (street[0][1],street[0][0])


def main():
    data=readfile("original_luc_gt_909.csv")
    print("finished reading data")

    streets=sort_streets(data)
    print("finished sorted street")

    data=compareOwnerNames(streets)
    print("finished comparing owner names")

    matchAgencyNames("MassGovernmentAgencyList.csv",data)

main()
