from procstream import StreamProcessMicroService
import os
import logging as logger
import spacy
from country_bboxes import get_ISO3166_citycode
from geopy.geocoders import Nominatim
from geoextract import GeoLookup, osm_lookup_place

config = {"MODULE_NAME": os.environ.get('MODULE_NAME', 'LEWS_LANG_DETECT'),
          "CONSUMER_GROUP": os.environ.get("CONSUMER_GROUP", "LEWS_LANG_DETECT_CG")}

class StreamProcessClassifyItalianTweets(StreamProcessMicroService):
    def __init__(self, config_new, italian_landslip_model, italian_rain_model, geo_lookup_object):
        super().__init__(config_new)
        self.italian_landslip_model = italian_landslip_model
        self.italian_rain_model = italian_rain_model
        self.geo_lookup_object = geo_lookup_object

    def process_message(self, message):
        payload = message.value
        if payload.get("lews_meta_detected_lang") == "it" \
                and payload.get("lang") == "it":
            print("开始process  意大利数据")
            payload["lews-meta-it_class_flag"] = "True"
            payload = self.classify_landslip(payload)
            payload = self.classify_rain(payload)
            payload = self.geo_extraction(payload)


            #logger.debug(payload)
        else:
            #logger.info("Not an italian tweet")
            return None
        return payload

    def classify_landslip(self, tweet_record):
        doc = self.italian_landslip_model(tweet_record.get('text'))
        tweet_record['lews-meta_is_landslip_related'] = 0
        if doc.cats['POSITIVE'] >= 0.5:
            tweet_record['lews-meta_is_landslip_related'] = 1
        return tweet_record

    def classify_rain(self, tweet_record):
        doc = self.italian_rain_model(tweet_record.get('text'))
        tweet_record['lews-meta_is_rainfall_related'] = 0
        if doc.cats['POSITIVE'] >= 0.5:
            tweet_record['lews-meta_is_rainfall_related'] = 1
        return tweet_record

    def data_clean(self, text):
        temp = text.replace('#', ',').replace('RT', '').replace('@', ',')
        return temp

    def remove_duplicate(self, places):
        visited = []
        for place in places:
            if place not in visited:
                visited.append(place)
        return visited

    def country_filter(self, extracted):
        in_the_country = dict()
        for place in extracted:
            places = osm_lookup_place(place)
            if len(places) != 0:
                in_the_country[place] = places
        return in_the_country

    # def geo_locate(self, tweet_record):
    #     logger.debug("geo_locate start !")
    #     cleaned_text = self.data_clean(tweet_record.get("text"))
    #     extracted = self.geo_lookup_object.get_geotag(cleaned_text)
    #     duplicate_removed = self.remove_duplicate(extracted)
    #     valid_places = self.country_filter(duplicate_removed)
    #     logger.debug("Valid place : ",valid_places)
    #     locations = []
    #     for place in valid_places:
    #         if place != (None, None):
    #             location = self.geo_lookup_object.geo_cache(place)
    #             if location != (None, None):
    #                 locations.append({'lat': location[1], 'lon': location[0]})
    #     if len(locations) > 0:
    #         tweet_record['lews-meta-it_location'] = locations
    #
    #     return tweet_record

    def geo_extraction(self, parm_data):

        data = parm_data

        # TODO 同样需要多种类型判断
        if type(data["text"]) is not float:
            cleaned_text = self.data_clean(data["text"])
            extracted = self.geo_lookup_object.get_geotag(cleaned_text)
            duplicate_removed = self.remove_duplicate(extracted)
            valid_places = self.country_filter(duplicate_removed)
            paired_location = []
            for place in valid_places:
                if place != (None, None):
                    location = self.geo_lookup_object.geo_cache(place)
                    if location != (None, None):
                        paired_location = location
            if len(paired_location) > 0:
                data['lews-metadata_longitude'] = paired_location[0]
                data['lews-metadata_latitude'] = paired_location[1]

                '''
                Geo reformat to iso 3166-2
                '''

                try:
                    geolocator = Nominatim(user_agent='myuseragent')
                    location = geolocator.reverse(str(paired_location[0])+','+str(paired_location[1]))

                    data['lews-meta-it_location_address'] = location.address

                    if "address" in location.raw:
                        if "city" in location.raw["address"]:
                            data['lews-meta-it_location_address_city'] = location.raw["address"]["city"]
                            data['lews-meta-it_location_address_city_ISO3166-2'] = get_ISO3166_citycode(
                                location.raw["address"]["city"])
                            if "country_code" in location.raw["address"] and location.raw["country_code"] == "it":
                                print("location address raw : ")
                                print(location.raw)
                                data['lews-meta-it_location_address_city_ISO3166-2_with_country'] = get_ISO3166_citycode(
                                "IT-" + location.raw["address"]["city"])
                        if "city_district" in location.raw["address"]:
                            data['lews-meta-it_location_address_city_district'] = location.raw["address"]["city_district"]

                except:
                    print('Geo info not found')
                    return data

        return data

    # def detection(self, model, new_col, data):
    #
    #     nlp = spacy.load(model)
    #
    #     # 初始化新列为0
    #     data[new_col] = 0
    #
    #     for index, row in data.iterrows():
    #         # TODO 可能有各种类型，需要检查
    #         if type(row["text"]) is not float:
    #             text = row["text"]
    #             doc = nlp(text)
    #             if doc.cats['POSITIVE'] >= 0.5:
    #                 data.loc[index, new_col] = 1
    #                 continue
    #
    #     return


def main():
    italian_landslip_model = spacy.load('italian_landslip_model')
    italian_rain_model = spacy.load('italian_rain_model')
    geo_lookup_object = GeoLookup()
    k_service = StreamProcessClassifyItalianTweets(config, italian_landslip_model, italian_rain_model,
                                                   geo_lookup_object)
    k_service.start_service()

if __name__ == "__main__":
    main()