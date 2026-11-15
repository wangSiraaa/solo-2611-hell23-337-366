from time_rules import normalize
from association import pairs
class Store:
 def __init__(self,events,rules):self.raw=events;self.rules=rules;self.events=[dict(e,tai=normalize(e,rules)) for e in events];self.pairs=pairs(self.events)
 def update_rules(self,rules):self.rules=rules
