from mrjob.job import MRJob


class MRSentenceCount(MRJob):

    def mapper(self, _, line):
        fields = line.strip().split(',')
        if fields[0] == 'region_name':
            return

        region_name = fields[0]
        city_name = fields[1]
        cpe_manufacturer_name = fields[2]
        cpe_model_name = fields[3]
        url_host = fields[4]
        cpe_type_cd = fields[5]
        cpe_model_os_type = fields[6]
        price = fields[7]
        date = fields[8]
        part_of_day = fields[9]
        request_cnt = fields[10]
        user_id = fields[11]

        yield (region_name, city_name, cpe_manufacturer_name, cpe_model_name, url_host, cpe_type_cd, cpe_model_os_type, price, date, part_of_day, request_cnt, user_id), 1


    def combiner(self, key, values):
        yield key, sum(values)
        
    def reducer(self, key, values):
        yield key, sum(values)


if __name__ == '__main__':
    MRSentenceCount.run()