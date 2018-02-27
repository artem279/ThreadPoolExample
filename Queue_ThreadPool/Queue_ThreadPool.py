import time
from threading import Thread
import threading
from queue import Queue
import csv, os
from lxml import etree, objectify

csv.register_dialect('csvCommaDialect', delimiter='|', lineterminator='\n')


class Task(Thread):
    def __init__(self, tasks):
        Thread.__init__(self)
        self.tasks = tasks
        self.daemon = True
        self.start()

    def run(self):
        while True:
            func, args, kwargs = self.tasks.get()
            try:
                func(*args, **kwargs)
            except Exception as e:
                print(e)
            finally:
                self.tasks.task_done()


class ThreadPool:
    """Pool of threads consuming tasks from a queue"""
    def __init__(self, num_threads):
        self.tasks = Queue(num_threads)
        for _ in range(num_threads):
            Task(self.tasks)

    def add_task(self, func, *args, **kwargs):
        """Add a task to the queue"""
        self.tasks.put((func, args, kwargs))

    def wait_completion(self):
        """block until all tasks are done"""
        self.tasks.join()


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


def getinn(**kwargs):
    try:
        doc = etree.parse(kwargs['file'])
    except Exception as e:
        print('Error!', kwargs['file'])
        pass

    root = cleannamespaces(doc.getroot())
    suppliers = []
    suppliers = [{'inn': s.text.strip()} for s in root.iter('INN') if s.text != '' and s.text is not None]
    if suppliers is not None:
        kwargs['mutex'].acquire()
        with open('data.csv', 'at', encoding='cp1251', errors='ignore') as file:
            writer = csv.DictWriter(file, ['inn'], dialect='csvCommaDialect')
            writer.writerows(suppliers)
        kwargs['mutex'].release()
    print('Thread %d is done!' % threading.get_ident())

    return None


def createthreadparser(thread_count, files):
    mutex = threading.Lock()
    pool = ThreadPool(int(thread_count))
    while len(files) != 0:
        file = files.pop()
        pool.add_task(getinn, file=file, mutex=mutex)
    pool.wait_completion()

    return None


files = getfiles('C:/xml_path/')
t0 = time.time()
createthreadparser(int(os.environ['NUMBER_OF_PROCESSORS']), files)
t = time.time() - t0
print('Понадобилось времени (сек.): %fs' % t)
with open('timelog.log', 'at', encoding='cp1251', errors='ignore') as file:
    file.write('Понадобилось времени (сек.): %fs' % t + '\n')
    file.flush()
