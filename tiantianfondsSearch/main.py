import os, pickle, time, json

def share_tasks(data):
    # slice tasks into multiple parts, each part contains 100 items
    # waiting to be crawled
    l = len(data)
    n = l//100 + 1
    step = 100
    tasks = {}
    data = list(data.values())
    for i in range(0, l, step):
        index = "%d--%d"%(i, i+step)
        tasks[index]= data[i:i+step]
    return tasks

# def read_files(files):
#     # after tasks been sliced, each part of task would be
#     # stored into pickle files, this function is to read those files
#     # and return
#     tasks = {}
#     for file in files:
#         name = file.split('_')[0]
#         with open(file,'rb') as f:
#             tasks[name] = pickle.load(f)
#     return tasks

def sort_jsonfiles():
    def read_json(files):
        res = []
        for file in files:
            with open(file, 'r',encoding='utf-8') as f:
                content = f.read()
            content = content.split('\n')
            final_content = []
            for c in content:
                try:
                    temp = json.loads(c)
                    final_content.append(temp)
                except json.JSONDecodeError as e:
                    # print('error decoding..')
                    pass
            res.extend(final_content)
        return res

    def write_json(title,data):
        with open(title+'.json','w',encoding='utf-8') as f:
            content = {title: data}
            json.dump(content, f)

    j_files = [_ for _ in os.listdir() if os.path.splitext(_)[1] == '.json']
    jbgk = [_ for _ in j_files if _.find('jbgk') != -1]
    jjcc = [_ for _ in j_files if _.find('jjcc') != -1]
    zqcc = [_ for _ in j_files if _.find('zqcc') != -1]
    jbgk_data = read_json(jbgk)
    jjcc_data = read_json(jjcc)
    zqcc_data = read_json(zqcc)
    write_json('jbgk',jbgk_data)
    write_json('jjcc',jjcc_data)
    write_json('zqcc',zqcc_data)
    r = [os.remove(_) for _ in j_files]

def sort_picklefiles():
    def read_pickle(files):
        final = {}
        for file in files:
            with open(file,'rb') as f:
                c = pickle.load(f)
            final.update(c)
        return final

    p_files = [_ for _ in os.listdir() if os.path.splitext(_)[1] == '.pl']
    p_files.remove('fundcodesData.pl')
    remained = read_pickle(p_files)
    with open('remained.pl','wb') as f:
        pickle.dump(remained, f)
    a = [os.remove(_) for _ in p_files]

def main():
    if not os.path.exists('0--100_fundcodes.pl'):
        with open('fundcodesData.pl','rb') as f:
            data = pickle.load(f)
        tasks = share_tasks(data)
        file_names = []
        for k, v in tasks.items():
            name = '%s_fundcodes.pl'%k
            with open(name, 'wb') as f:
                pickle.dump(v, f)
            file_names.append(name)
    else:
        file_names = [_ for _ in os.listdir() if os.path.splitext(_)[1]=='.pl']
        file_names.remove('fundcodesData.pl')
        # tasks = read_files(file_names)
    print('\n总共 %d 个任务\n'%len(file_names))
    num = 1
    for each in file_names:
        print('启动第 %d 个爬虫任务'%num)
        os.popen('scrapy crawl fundspider -a file=%s -a id=%02d'%(each, num))
        num += 1
        time.sleep(1)

if __name__ == "__main__":
    main()
