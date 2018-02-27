from multiprocessing.pool import ThreadPool
from multiprocessing import Lock
import threading
from lxml import etree, objectify
import os, csv, time

csv.register_dialect('csvCommaDialect', delimiter='|', lineterminator='\n')


def getfiles(path):
    filearray = []
    for top, dirs, files in os.walk(path):
        for nm in files:
            filearray.append(os.path.join(top, nm))
    return filearray


def cleannamespaces(root):
    for elem in root.getiterator():
        if not hasattr(elem.tag, 'find'):
            continue  # (1)
        i = elem.tag.find('}')
        if i >= 0:
            elem.tag = elem.tag[i + 1:]

    objectify.deannotate(root, cleanup_namespaces=True)

    return root


def getvalue(tag):
    value = ''
    try:
        value = tag.replace('\n', '').replace('\r', '').replace('\t', '').replace('|', '/') \
            .replace('"', '').replace('  ', '').replace('NULL', '').replace(';', ',')
    except:
        value = ''
    return value


def getinn(file):
    try:
        doc = etree.parse(file)
    except Exception as e:
        print('Error!', file)
        pass
    mutex = Lock()
    root = cleannamespaces(doc.getroot())
    suppliers = []
    suppliers = [{'inn': s.text.strip()} for s in root.iter('INN') if s.text != '' and s.text is not None]
    if suppliers is not None:
        mutex.acquire()
        with open('data.csv', 'at', encoding='cp1251', errors='ignore') as file:
            writer = csv.DictWriter(file, ['inn'], dialect='csvCommaDialect')
            writer.writerows(suppliers)
        mutex.release()
        print('Thread %d is done!' % threading.get_ident())

    return None


def createthreadparser(thread_count, files):
    pool = ThreadPool(int(thread_count))
    pool.map_async(getinn, files)
    pool.close()
    pool.join()

    return None


files = getfiles('C:/xml_path/')
t0 = time.time()
createthreadparser(int(os.environ['NUMBER_OF_PROCESSORS']), files)
t = time.time() - t0
print('Понадобилось времени (сек.): %fs' % t)
with open('timelog.log', 'at', encoding='cp1251', errors='ignore') as file:
    file.write('Понадобилось времени (сек.): %fs' % t + '\n')
    file.flush()

