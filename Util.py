#_*_ coding: utf-8 _*_

import pickle

def save_pickle(file='test.pickle', obj=None):
    f = open(file, 'wb')
    pickle.dump(obj, f)
    f.close()

    return True

def read_pickle(file='test.pickle'):
    f = open(file, 'rb')
    obj = pickle.load(f)
    f.close()

    return obj