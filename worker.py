from rq import Connection, Queue, Worker
import multiprocessing

def start_worker(i: int):
    with Connection():
        q = Queue()
        Worker(q).work()

if __name__ == '__main__':
   with multiprocessing.Pool(20) as p:
       p.map(start_worker, list(range(20)))
