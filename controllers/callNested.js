const nestedWrapper = require('./nested').nestedWrapper;

ref_keys = ['mor_id', 'prov_id']

data = {
  'children': [
  ],
}

docs = [
  {
    'mor_id': 1,
    'prov_id': 2,
    'y_sale': 20,
  },
  {
    'mor_id': 3,
    'prov_id': 2,
    'y_sale': 20,
  },
]


result = nestedWrapper(data, ref_keys, docs)
console.log('Print Here')
