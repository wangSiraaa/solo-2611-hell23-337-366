def validate(spec):
 return spec.get('sort')=='score_desc_id_asc' and spec.get('max_page',0)>0
