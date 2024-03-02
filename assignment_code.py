import redis
import json
import matplotlib.pyplot as plt

class JsonProcessor:
    """
    Above class will doing the followings:
    1. Read json data using api_url
    2. Insert into RedisJSON
    3. Performing some operations
    """

    def __init__(self, api):
        """
        This function will intialize JsonProcessor with api_url.
        It will also make connection with Redis using host,port and database number.
        """
        self.api = api
        self.redis_server = redis.StrictRedis(host='localhost', port=6379, db=0)


    def read_json(self):
        """
        This function will read data from given url api_url.
        Return
        str: fetched data
        """
        response = requests.get(self.api)
        if response.status_code == 200:
            return response.json()
        else:
            print("Failed: Not able to extract data")
            return None

    def insert_into_redis_db(self, key, data):
        """
        This function will insert json data into redis using key and data
        Parameters
        key(str): key to store json data
        data(str): json data to be insertd in Redis
        """
        self.redis_server.set(key, data)

    def create_bar_chart(self,key, data):
        """
        This dunction will create a bar graph
        Parameters:
        key (str) : Key for which we want to showcase data
        data(str): json data
        """
        dict=data[key]
        x = list(dict.keys())
        y = list(dict.values())
        plt.bar(x, y)
        plt.xlabel('userId')
        plt.ylabel('No of Titles')
        plt.title('Bar Chart')
        plt.show()

    def perform_aggregation(self, key, data):
        """
        This function will perform aggregation

        Parameters:
        key (str) : Key for which we want to perform aggregation
        data(str): json data

        Return:
        result (float): It will return the average of the given value

        """
        result = sum(data[key])/len(data[key])
        # result = self.redis_server.execute_command('JSON.AGGREGATE', key, '.', 'AVG', field)
        return result

    def perform_search(self, query, data):
        """
        This function will search data

        Parameters:
        query (str) : query for which we want to search data
        data(str): json data

        Return:
        result ([]): It will return the searched result

        """
        result=[]
        for i in data:
            if i['userId']==query:
                result.append(i)

        # result = self.redis_server.execute_command('JSON.QUERY', key, query)
        return result

def main():
    api = 'https://jsonplaceholder.typicode.com/posts'

    # Initialize DataProcessor
    processor = JsonProcessor(api)

    # Fetch JSON data from API
    json_data = processor.read_json()
    if json_data:
        # Insert JSON data into RedisJSON
        processor.insert_into_redis_db('data', json.dumps(json_data))
        # Insert dummy data into RedisJSON
        json_data_lst = {'list':[1,2,3,4]}
        processor.insert_into_redis_db('lst', json.dumps(json_data_lst))
        # Insert dummy data into RedisJSON
        json_data_dct = {'dict':{'1':4,'2':3,'3':5,'4':2}}
        processor.insert_into_redis_db('dct', json.dumps(json_data_dct))
        # Create a simple bar chart
        processor.create_bar_chart('dict',json_data_dct)
        # Perform aggregation
        aggregation_result = processor.perform_aggregation('list', json_data_lst)
        print("Average Result:", aggregation_result)
        # Perform search
        search_result = processor.perform_search(1, json_data)
        print("Search Result:", search_result)

if __name__ == "__main__":
    main()
