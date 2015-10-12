#!/usr/bin/python
# MDreader
# Copyright (c) Manuel Nuno Melo (m.n.melo@rug.nl)
#
# Released under the GNU Public Licence, v2 or any higher version
#
import collections
import numpy


# RegisteredFunction Class #############################################
########################################################################

class RegisteredFunction():
    def __init__(self, fn, name=None, parent_col=None, rettype="python", fn_args=(), fn_kwargs=dict()):
        if not callable(fn):
            raise TypeError("Attempted to register as a function an object that is not callable")
        self.fn = fn
        self.args = fn_args
        self.kwargs = fn_kwargs
        self.name = None
        if name is None:
            self.name = igetattr(fn,"__name__", None)
        self.make_name_unique()
        if rettype != "auto":
            self.rettype = rettype
        else:
            self.get_rettype()

    def __call__(self, *args, **kwargs):
        return self.fn(*args, **kwargs)

    def make_name_unique(self):
        name = self.name
        num = 1
        if fullname is None:
            name = "Function"
        if self.parent_col is None:
            self.name = name
            return
        else:
            while self.parent_col.fn_list.has_key(fullname):
                num += 1
                fullname = "%s_%d" % (name, num)
            self.name = fullname

def get_rettype(self, fn, fn_args, fn_kwargs):
    ret = fn(*fn_args, **fn_kwargs)
    if ret is None:
        return None
    if type(ret) == numpy.ndarray:
        rettype = [[]] 
    rettype = []






        

class FunctionList():
    def __init__(self):
        self.fn_list = collections.Ordereddict([])

    def __len__(self):
        return len(self.fn_list)

    def register(self, *args, **kwargs):
        kwargs["parent_col"] = self
        fn = RegisteredFunction(*args, **kwargs)
        self.fn_list[fn.name] = fn
        
        

