from procstream import StreamProcessMicroService
import os
import logging as logger
import spacy
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
                or payload.get("lang") == "it":
            payload["lews-meta-it_class_flag"] = "True"
            payload = self.classify_landslip(payload)
            payload = self.classify_rain(payload)
            payload = self.geo_locate(payload)
        else:
            payload["lews-meta-it_class_flag"] = "NA"
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

    def geo_locate(self, tweet_record):
        cleaned_text = self.data_clean(tweet_record.get("text"))
        extracted = self.geo_lookup_object.get_geotag(cleaned_text)
        duplicate_removed = self.remove_duplicate(extracted)
        valid_places = self.country_filter(duplicate_removed)
        locations = []
        for place in valid_places:
            if place != (None, None):
                location = self.geo_lookup_object.geo_cache(place)
                if location != (None, None):
                    locations.append({'lat': location[1], 'lon': location[0]})
        if len(locations) > 0:
            tweet_record['lews-meta-it_location'] = locations
        return tweet_record

    # def geo_extraction(self, data):
    #
    #     # 创建 两个列  经纬度
    #     data['lews-metadata_longitude'] = None
    #     data['lews-metadata_latitude'] = None
    #
    #     for index, row in data.iterrows():
    #         # TODO 同样需要多种类型判断
    #         if type(row["text"]) is not float:
    #             cleaned_text = self.data_clean(row["text"])
    #             extracted = self.geo_lookup_object.get_geotag(cleaned_text)
    #             duplicate_removed = self.remove_duplicate(extracted)
    #             valid_places = self.country_filter(duplicate_removed)
    #             paired_location = []
    #             for place in valid_places:
    #                 if place != (None, None):
    #                     location = self.geo_lookup_object.geo_cache(place)
    #                     if location != (None, None):
    #                         paired_location = location
    #             if len(paired_location) > 0:
    #                 data.loc[index, ('lews-metadata_longitude', 'lews-metadata_latitude')] = paired_location
    #
    #     return

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
