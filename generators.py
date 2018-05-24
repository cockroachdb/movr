import uuid, random
#@todo: how to do this in the database?

class MovRGenerator:
    def generate_uuid(self):
        return str(uuid.uuid4())

    def generate_revenue(self):
        return random.uniform(1,100)

    def generate_random_vehicle(self):
        return random.choice(['skateboard', 'bike', 'scooter'])

    def generate_random_color(self):
        return random.choice(['red', 'yellow', 'blue', 'green', 'black'])

    def gen_bike_brand(self):
        return random.choice(['Merida','Fuji'
        'Cervelo', 'Pinarello',
        'Santa Cruz', 'Kona', 'Schwinn'])

    def generate_vehicle_metadata(self, type):
        metadata = {}
        metadata['color'] = self.generate_random_color()
        if type == 'bike':
            metadata['brand'] = self.gen_bike_brand()
        return metadata

    def weighted_choice(self, items):
        """items is a list of tuples in the form (item, weight)"""
        weight_total = sum((item[1] for item in items))
        n = random.uniform(0, weight_total)
        for item, weight in items:
            if n < weight:
                return item
            n = n - weight
        return item