class Nested:
    def __init__(self, ref_keys, saved_keys, **kwargs):
        self.pivots = kwargs.get('pivot_data', [])
        self.skip_keys = kwargs.get('skip_keys', [])
        self.data = {
            'children': {}
        }
        self.ref_keys = ref_keys
        self.ref_len = len(self.ref_keys)
        self.pivot_keys = [
            'y_sale', 'y_targ', 'y_perf', 'm_sale', 'm_targ', 'm_perf',
            'd_sale', 'd_targ', 'd_perf'
        ]
        self.saved_keys = saved_keys
        self.saved_keys[-1] += (self.pivot_keys)
        pivot_data = self.get_pivot_data(-1, self.data)
        pivot_data = dict([(k, v)for (k, v) in pivot_data.items() if k in self.pivot_keys])
        self.data.update(pivot_data)

    def output(self):
        def flats(data):
            if data.get('children', False):
                temp = []
                for value in data['children'].values():
                    temp.append(flats(value))
                data['children'] = temp
            # else:
            #     print("Pojok")
            return data

        elems = self.data
        final_result = []
        for keyx, valuex in elems['children'].items():
            mantap = flats(valuex)
            final_result.append(mantap)

        # print(final_result)
        return final_result

    def get_pivot_data(self, index, row):
        selected_pivot = False
        if index == -1:
            key = '{'+','.join(str(0) for x in self.ref_keys)+'}'
        else:
            zzz = []
            for index_key, key in enumerate(self.ref_keys):
                if index_key <= index:
                    zzz.append(row[key])
                else:
                    zzz.append(0)
            key = '{'+','.join(str(x) for x in zzz)+'}'
        res = self.pivots.pop(key, {})
        return res

    def add(self, row):
        self.process(self.data, -1, row)

    def rename_data_keys(self, data, ref_key):
        for (k, v) in list(data.items()):
            if self.skip_keys and k in self.skip_keys:
                continue
            if k == ref_key:
                data['id'] = v
                data.pop(k)
            elif k == ref_key.split('_id')[0]+'_name':
                data['name'] = v
                data.pop(k)
            elif k == ref_key.split('_')[0]+'_lng':
                data['lng'] = v
                data.pop(k)
            elif k == ref_key.split('_')[0]+'_lat':
                data['lat'] = v
                data.pop(k)
        return data

    def process(self, doc, index, row):
        if index == -1:  # untuk Root
            self.process(doc, index+1, row)

        elif index < self.ref_len:  # Selama Bukan Edge
            ref_key = self.ref_keys[index]
            key = row.get(ref_key, False)
            if not key:
                return
            create_new = True
            elem = doc['children'].get(key, False)
            if elem:
                create_new = False
            if not create_new:
                self.process(elem, index+1, row)
            else:
                data = dict([(k, v)for (k, v) in row.items() if k in self.saved_keys[index]])
                pivot_data = self.get_pivot_data(index, row)
                if pivot_data:
                    data.update(pivot_data)
                data.update({
                    ref_key: key,
                }),
                data = self.rename_data_keys(data, ref_key)
                data.update(
                    {
                        'children': {},
                    }
                )
                doc['children'][key] = data
                elem = doc['children'][key]
                self.process(elem, index+1, row)

        else:  # Khusus Edge
            data = dict([(k, v)for (k, v) in row.items() if k in self.saved_keys[-1]])
            last_ref_key = 'product_id'
            data = self.rename_data_keys(data, last_ref_key)
            vals = data
            doc['children'][row['product_id']] = vals
