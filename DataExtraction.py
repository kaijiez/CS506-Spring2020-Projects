import http.client
import json

"""use Attom API to retreive information about a property"""

"""return property profile in json format given address"""
def GetPropertyDetails(address="street,city,state"):

    address = address.replace(" ","%20").split(",")
    """need to have the full address(street, city, state)"""
    address1 = address[0]
    address2 = address[1]+"%2C"+address[2]

    conn = http.client.HTTPSConnection("api.gateway.attomdata.com")
    payload = ''
    headers = {
      'accept': "application/json",
      'apikey': '77ef4fa4d5c4a6eb62f8afda13b76406'
    }
    # conn.request("GET", "/propertyapi/v1.0.0/property/basicprofile?address1=4529%20Winona%20Court&address2=Denver%2C%20CO", headers=headers)
    conn.request("GET", "/propertyapi/v1.0.0/property/basicprofile?address1="+address1+"&address2="+address2, headers=headers)
    res = conn.getresponse()
    data = res.read()
    print(data.decode("utf-8"))
    return data.decode('utf-8')

"""get [latitude,longitude] from property profile"""
def GetLatLong(profile="json"):
    profile=json.loads(profile)
    return [profile.get("property")[0].get("location").get("latitude"),profile.get("property")[0].get("location").get("longitude")]


"""return list of areas in json format"""
def GetAreas(lat="lat",long="long"):
    conn = http.client.HTTPSConnection("api.gateway.attomdata.com")
    headers = {
        'accept': "application/json",
        'apikey': '77ef4fa4d5c4a6eb62f8afda13b76406'
    }
    conn.request("GET", "/areaapi/v2.0.0/hierarchy/lookup?latitude="+str(lat)+"&longitude="+str(long),headers=headers)

    res = conn.getresponse()
    data = res.read()
    print(data.decode("utf-8"))
    return data.decode('utf-8')

"""take in areas in json format and return [ids] for neighborhoods"""
def GetNeighborhoodID(areas="json"):
    areas = json.loads(areas)
    list = areas.get("response").get("result").get("package").get("item")

    """only retrieve neighborhood areas"""
    ids=[]
    for area in list:
        if area.get("type")=="ND":
            ids.append(area.get("id"))
    return ids


def GetNeighborhoodInfo(id=""):
    conn = http.client.HTTPSConnection("api.gateway.attomdata.com")
    headers = {
        'accept': "application/json",
        'apikey': '77ef4fa4d5c4a6eb62f8afda13b76406'
    }
    conn.request("GET", "/communityapi/v2.0.0/Area/Full?AreaId="+id,headers=headers)

    res = conn.getresponse()
    data = res.read()
    print(data.decode("utf-8"))
    return data.decode('utf-8')


# coord=GetLatLong(GetPropertyDetails("4529 Winona Court,Denver,CO"))
# GetNeighborhoodID(GetAreas(coord[0],coord[1]))
GetNeighborhoodInfo("CO44003")