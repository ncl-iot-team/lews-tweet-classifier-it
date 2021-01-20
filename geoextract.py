import requests
import traceback
import spacy
# from spacy.lang.en import English
import json
import country_bboxes


class GeoLookup:
    def __init__(self):
        self.chunksize = 1000
        self.output_fields = 'text'
        self.gcache = dict()
        # TODO 每次都需要http请求   很慢  待优化
        self.url_template = "https://photon.komoot.de/api/?q=%s"
        # self.url_template = "http://localhost:2322/api/?q=%s"
        #  self.nlp = spacy.load("it_core_news_sm")
        self.nlp = spacy.load("it_core_news_lg")

    # ----Spacy Functions --------------
    # Helps in converting the first letter of each sentence to capital
    @staticmethod
    def preprocess(self, text):
        return text.title()

    # Function which helps in fetching the geotag from the source input file
    def get_geotag(self, text):
        gtag = []
        doc = self.nlp(text)

        for E in doc.ents:
            if E.label_ in ['GPE', 'LOC']:
                if self.output_fields == 'text':
                    gtag.append(E.text)
                elif self.output_fields == 'label':
                    gtag.append(E.label_)
                else:
                    gtag.append((E.text, E.label_))
        return gtag

    # Function to run the whole process first capitalising the first letter than finding geotags from input
    def process(self, text):
        ptext = self.preprocess(text)
        return self.get_geotag(ptext)

    # -------------------------------------------------------------

    # Function which saves converted geo-coordinates in cache so the program should not contact OSM server for getting
    # information of repeated geotags
    def geo_cache(self, place):
        if place in self.gcache:
            return self.gcache[place]
        ## entry not found. update cache
        tags = self.osm_coordinates(place)
        self.gcache[place] = tags
        return tags

    # Connects to the service and check whether return value is city, country, state and town
    # from the input text which are tweets to fetch latitudes and longitudes
    def osm_coordinates(self, place):
        not_found = (None, None)
        req = requests.get(self.url_template % place)
        try:
            data = req.json()
            for d in data['features']:
                if 'place' != d['properties']['osm_key']:
                    continue
                if not d['properties']['osm_value'] in ['country', 'city', 'county', 'village', 'town']:
                    continue
                lon = d['geometry']['coordinates'][0]
                lat = d['geometry']['coordinates'][1]
                if check_if_in_italy(lon, lat):
                    return (lon, lat)
                else:
                    continue
        except:
            ## errors when spacy return wrong names for places which has unsupported characters like '#Endomondo'
            return not_found
        return not_found

    # Longitude, Latitude: add the bounding box to check if the tweet is from Italy

    # Each input line calls process function runs spacy to fetch geotags then convert them
    # into geo-coordinates and store it in mentions list and create require structure
    def parse_input(self, data):
        text = data['text']
        timestamp = data['timestamp_ms']
        plist = self.process(text)
        mentions = self.places_to_geo_coordinates(plist)

        try:
            result = self.create_output_struct(text, timestamp, mentions)
        except Exception as err:
            print(err)
            print(traceback.print_exc())
            return []

        return (result)

    # Extracts all geo-places checks for cache if converted lat and long is present for that particular
    # geotag if present accesses it else connect to service to get geo-coordinates

    def places_to_geo_coordinates(self, places):
        gtags = []
        for p in places:
            if len(p) > 0:
                tags = self.geo_cache(p)
                if not tags[0] is None:  # filter out things which spacy incorrectly detected  as a place
                    gtags.append((p, tags[0], tags[1]))
        return gtags

    # To create a JSON format output strcture which makes it easy
    # for indexing in elastic search
    def create_output_struct(self, text, timestamp, user_mentions):
        result = {'media': 'twitter'}
        result['time of publish'] = timestamp
        result['raw_data'] = text
        mentions = []
        print(user_mentions)
        for m in user_mentions:
            mentions.append({'place': m[0], 'geotag': {'lat': m[1], 'lon': m[2]}})
        result['mentions'] = mentions
        return result

    # Pandas data analytics framework to access data easily
    def process_input(self):
        f = open('test.txt', 'r')
        input = f.read()
        result = self.parse_input(json.loads(input))
        return result


def check_if_in_italy(Longitude, Latitude):
    # minimum longitude,  minimum latitude,  maximum longitude, maximum latitude
    bbox = country_bboxes.get_country_bounding_boxes('IT')[1]
    if float(Longitude) >= bbox[0] and float(Longitude) <= bbox[2]:
        if float(Latitude) >= bbox[1] and float(Latitude) <= bbox[3]:
            return True
        else:
            return False
    else:
        return False

def osm_lookup_place(place):
    # url_template = "https://nominatim.openstreetmap.org/search/%s?format=json&addressdetails=1&limit=1&polygon_svg=1"
    url_template = "https://nominatim.openstreetmap.org/search/%s?format=json&addressdetails=1"
    valid_place = []
    req = requests.get(url_template % place)
    try:
        data = req.json()
        for i in range(len(data)):
            found = data[i]
            location = found['address']
            if location['country_code'] == "it":
                valid_place.append(found)
            else:
                lon = found['lon']
                lat = found['lat']
                if check_if_in_italy(lon, lat):
                    valid_place.append(found)
        return valid_place
        # print(valid_place)
    except:
        ## errors when spacy return wrong names for places which has unsupported characters like '#Endomondo'
        return valid_place

# Main function which creates object and runs whole process
if __name__ == "__main__":
    nlp_obj = GeoLookup()
    nlp_obj.process_input()
