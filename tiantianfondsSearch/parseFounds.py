import re
pattern = re.compile('\],\[')

def get_list(data, pattern):
    res = []
    position = pattern.search(data)
    while position:
        index = position.span()
        res.append(data[:index[0]+1])
        data = data[index[1]-1:]
        position = pattern.search(data)
    return res

def finalfundcodes(secondata):
    final = [eval(_) for _ in secondata]
    return final
