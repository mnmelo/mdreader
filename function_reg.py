#!/usr/bin/python
# MDreader
# Copyright (c) Manuel Nuno Melo (m.n.melo@rug.nl)
#
# Released under the GNU Public Licence, v2 or any higher version
#
import collections
import numpy
import types


# RegisteredFunction Class #############################################
########################################################################

class RegisteredFunction():
    def __init__(self, parent_collection=None, fn, name=None, nret=1, rettype=[("python",(1,))], fn_args=(), fn_kwargs=dict()):
        if not callable(fn):
            raise TypeError("Attempted to register as a function an object that is not callable")
        self.fn = fn
        self.args = fn_args
        self.kwargs = fn_kwargs
        self.name = None
        if name is None:
            self.name = getattr(fn,"__name__", None)
        self.make_name_unique()
        if rettype != "auto":
            self.rettype = rettype
        else:
            self.rettype = get_rettype()
        self.nret = len(self.rettype)
        ## can we use the enhanced memory model?
        ##self.emm = self.rettype[0][0] != "python"
        self.res = FunctionResult()

    def __call__(self, *args, **kwargs):
        return self.fn(*args, **kwargs)

    def default_call(self):
        return self(*self.args, **self.kwargs)

    def init_res(self, nframes):
        self.res.initialize(self.rettype, nframes)

    def repoint_res(self):
        self.res.recreate_arrays_fromraw()

    def make_name_unique(self):
        num = 1
        if self.name is None:
            self.name = "Function"
        if self.parent_colllection is None:
            return
        else:
            fullname = self.name
            while self.parent_collection.fn_list.has_key(fullname):
                num += 1
                fullname = "%s_%d" % (self.name, num)
            self.name = fullname

class FunctionResult(list):
    def initialize(self, rettype, nframes):
        """This initializes the arrays.
        Remote, non-local workers call this with the number of frames they were passed.
        """
        #try:
        #    if self.initialized:
        #        return
        #except AttributeError:
        #    self.initialized = False
        self.nframes = nframes
        self.rettype = rettype
        self.r_ctypes = []
        for ctype, shape in self.rettype:
            if ctype != "python":
                #We're using the EMM
                self.r_ctypes.append(multiprocessing.RawArray(ctype, numpy.product(shape)*nframes))
                self.append(numpy.ctypeslib.as_array(self.r_ctypes[-1]))
                self[-1] = self[-1].reshape((nframes,)+shape)
            else:
                self.r_ctypes.append(None)
                self.append(range(self.nframes))
        self.initialized = True

    def recreate_arrays_fromraw(self):
        try:
            if not self.initialized:
                raise AttributeError
        except AttributeError:
            raise AttributeError("Tried to recreate arrays but the pointers haven't yet been initialized by initialize()")
        for i, (ctype, shape) in self.rettype:
            if ctype != "python":
                self[i] = numpy.ctypeslib.as_array(self.r_ctypes[i])
                self[i] = self[i].reshape((self.nframes,)+shape)

class FunctionList(collections.Ordereddict):
    def register(self, *args, **kwargs):
        """The register function passes its arguments to the initialization of RegisteredFunction, with the addition that it sets parnt_collection to the current collection
        """
        fn = RegisteredFunction(*args, parent_collection=self, **kwargs)
        self[fn.name] = fn

    @property
    def result(self):
        return tuple([fn.res for fn in self.values()])

    def repoint_resarrays(self):
        for fn in self.values():
            fn.res.recreate_arrays_fromraw()



def get_rettype(fn, fn_args=(), fn_kwargs=dict()):
    """Function to get the return type of a function.

    Returns a list of types as returned by get_type. This is always a list of types, even if there is a single return value.
    However this is only so if all the types are translatable to ctypes. Otherwise a single element of type "python" and shape (1,) will be reported.
    """
    ret = fn(*fn_args, **fn_kwargs)
    typ = get_type(ret, go_deep=True)
    if not is_iter(typ[0]):
        typ = [typ]
    if any([t==numpy.object for t,shp in typ]):
        return [("python",(1,))]
    else:
        return typ

def get_type(obj, go_deep=False):
    """Function to get the type of an object, with the option of recursing if it is iterable

    If no recursion is performed, returns a 2-element tuple with the type and shape of the object.
    If recursion is asked for, get_type only goes one level deep. It then checks the types of all the sub objects. If they are the same,
     returns a 2-element tuple, just like the case above. Otherwise, a list of 2-element tuples is provided, one for each sub object.
    """
    try:
        # this is the best I could find for an automated numpy-ctypes translation; got it from the source of ctypeslib.as_ctypes
        ct = numpy.ctypeslib._typecodes[obj.__array_interface__["typestr"]]
        return ct, obj.shape
    except AttributeError:
        # doesn't have an array interface or .shape
        if is_iter(obj):
            if go_deep:
                typ = [get_type(subobj) for subobj in obj ]
                if not all_same(typ):
                    return typ
            return get_type(numpy.array(obj))
        else:
            return get_type(numpy.array([obj]))
    except KeyError:
        # doesn't have a translation into ctypes
        return numpy.object, obj.shape

def is_iter(obj):
    """A handy funcion to identify non-string iterables.
    """
    return not isinstance(obj, str) and isinstance(obj, collections.Iterable)

def all_same(lst):
    return lst.count(lst[0]) == len(lst)
        

