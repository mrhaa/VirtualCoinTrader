#_*_ coding: utf-8 _*_

import os
import sys
import time

import pandas as pd

def LearnerStart(obj):
    obj.Start()

class Learner(object):
    def __init__(self):
        print('Learner Generate.')

    def Start(self):
        print('Learner Start.')


class NeuralNet(Learner):
    def __init__(self):
        print('NeuralNet Generate.')

    def __del__(self):
        print('NeuralNet Terminate.')

    def Start(self):
        print('NeuralNet Start.')

        loop_cnt = 0
        while True:
            print('NeuralNet is learning(%s).'%(loop_cnt))

            loop_cnt += 1
            time.sleep(5.0)

