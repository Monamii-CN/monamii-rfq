from json import JSONEncoder


class RfqDetail:
    def __init__(self, title, quantity, unit, stars, open_time, origin, buyer, buyer_tag, quote_left, description,
                 link):
        self.title = title
        self.quantity = quantity
        self.unit = unit
        self.stars = stars
        self.open_time = open_time
        self.origin = origin
        self.buyer = buyer
        self.buyer_tag = buyer_tag
        self.quote_left = quote_left
        self.description = description
        self.link = link


class RfqDetailEncoder(JSONEncoder):
    def default(self, o):
        return o.__dict__
