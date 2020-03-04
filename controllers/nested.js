function nestedWrapper(data, ref_keys, docs, pivot) {

  ref_len = ref_keys.length
  pivot_keys = [
    'y_sale', 'y_targ', 'y_perf', 'm_sale', 'm_targ', 'm_perf',
    'd_sale', 'd_targ', 'd_perf'
  ]

  function get_pivot_data(row, index) {
    if (index == -1) {
      key_list = Array.from(new Array(4), (x, i) => 0)
    }
    else {
      key_list = []
      ref_keys.forEach((key, index_key) => {
        if (index_key <= index) {
          key_list.push(row[key])
        }
        else {
          key_list.push(0)
        }
      });
    }
    key = '{' + key_list.join(',') + '}'
    return pivot[key]
  }

  function get(object, key, default_value) {
    var result = object[key];
    return (typeof result !== "undefined") ? result : default_value;
  }
  function last(array) {
    return array[array.length - 1];
  }
  function has_key(elem, ref_key, value) {
    if (get(elem, ref_key, false)) {
      if (elem[ref_key] == value) {
        return true
      }
    }
  }

  function process(doc, index, row) {
    if (index == -1) {
      process(doc, index + 1, row)
    }
    else if (index < ref_len) {
      ref_key = ref_keys[index]
      ref_value = row[ref_key]
      create_new = true
      for (var i = 0, n = doc['children'].length; i < n; ++i) {
        elem = doc['children'][i]
        if (has_key(elem, ref_key, ref_value)) {
          create_new = false
          break
        }
      }
      if (!create_new) {
        process(elem, index + 1, row)
      }
      else {
        vals = get_pivot_data(row, index)
        if (vals == undefined) {
          vals = {}
        }
        vals[ref_key] = ref_value
        vals['children'] = []
        doc['children'].push(vals)
        elem = last(doc['children'])
        process(elem, index + 1, row)
      }
    }
    else {
      validKeys = pivot_keys + ['product_id', 'product_name']
      Object.keys(row).forEach((key) => validKeys.includes(key) || delete row[key]);
      doc['children'].push(row)
    }
  }

  pivot_data = get_pivot_data(data, -1)
  for (var i = 0, n = docs.length; i < n; ++i) {
    doc = docs[i]
    process(data, -1, doc)
  }
  return data
}

exports.nestedWrapper = nestedWrapper
