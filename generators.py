import uuid, random
#@todo: how to do this in the database?


#@todo: we shouldnt repeat the word generator in the class methods
class MovRGenerator:
    @staticmethod
    def generate_uuid():
        return str(uuid.uuid4())

    @staticmethod
    def generate_revenue():
        return random.uniform(1,100)

    @staticmethod
    def generate_random_vehicle():
        return random.choice(['skateboard', 'bike', 'scooter'])

    @staticmethod
    def get_vehicle_availability():
        return MovRGenerator.weighted_choice([("available", .4), ("in_use", .55), ("lost", .05)])

    @staticmethod
    def generate_random_color():
        return random.choice(['red', 'yellow', 'blue', 'green', 'black'])

    @staticmethod
    def generate_random_latlong():
        return {'lat': random.uniform(-180, 180), 'long': random.uniform(-90, 90)}


    @staticmethod
    def gen_bike_brand():
        return random.choice(['Merida','Fuji'
        'Cervelo', 'Pinarello',
        'Santa Cruz', 'Kona', 'Schwinn'])

    @staticmethod
    def generate_vehicle_metadata(type):
        metadata = {}
        metadata['color'] = MovRGenerator.generate_random_color()
        if type == 'bike':
            metadata['brand'] = MovRGenerator.gen_bike_brand()
        return metadata

    @staticmethod
    def weighted_choice(items):
        """items is a list of tuples in the form (item, weight)"""
        weight_total = sum((item[1] for item in items))
        n = random.uniform(0, weight_total)
        for item, weight in items:
            if n < weight:
                return item
            n = n - weight
        return item