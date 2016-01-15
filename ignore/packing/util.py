import math,random,types

def get_timestring():
    import time
    #print time.localtime()
    year,month,date,hour,min,sec,_,_,_=time.localtime()
    timestr='%d-%02d-%02d-%02d-%02d-%02d'%(year,month,date,hour,min,sec)
    return timestr
    
#=================CLASSES

class Bag(object):
    pass

#================================MANIP

def add(a,b):
    return (a[0]+b[0],a[1]+b[1])
    
def get_dist(center,spot):
    return math.hypot(center[0]-spot[0],center[1]-spot[1])

def area(obj):
    return obj.w*obj.h

def get_neighbors(pair):
    x,y=pair
    return ((x+1,y),(x-1,y),(x,y+1),(x,y-1))

def oob(im,pair):
    if pair[0]<0 or pair[1]<0 or pair[0]>=im.size[0] or pair[1]>=im.size[1]:
        return 1
    return 0

def jump(this,dir,dist):
    if dir==0:
        return (this[0]+dist,this[1])
    if dir==1:
        return (this[0],this[1]+dist)
    if dir==2:
        return (this[0]-dist,this[1])
    if dir==3:
        return (this[0],this[1]-dist)

def halfchoose(n):
    """forces x.5 values to x or x+1, randomly.  for int,float, or list,tuple of them"""
    if type(n)==types.FloatType or type(n)==types.IntType or type(n)==types.LongType:
        if n%0.5==0 and n%1!=0:
            n=n+random.choice([0.5,-0.5])
        return n
    elif type(n)==types.StringType or type(n)==types.UnicodeType:
        return n
    else:
        new=[]
        for e in n:
            e=halfchoose(e)
            new.append(e)
    return new

def ir(n):
#~     print n
    n=halfchoose(n)
#~     print 'aft'
#~     print n
    new=[]
    if type(n)==types.FloatType or type(n)==types.IntType or type(n)==types.LongType:
        n=int(round(n))
        return n

    else:
        for e in n:
            if type(e)==types.StringType or type(e)==types.UnicodeType:
                pass
            else:
                e=int(round(e))
            new.append(e)
        return new

#~ print ir([5.5,5.5,5,'apple'])
#~ AF

#=================COLORS

def lighten(triple):
    return tuple([min(n+150,255) for n in triple])

def darken(triple):
    return tuple([max(n-150,0) for n in triple])

def whiten(triple):
    return (255,254,255)

def blacken(triple):
    return (0,1,0)

def equalize(triple):
    return (triple[0],triple[0],triple[0])

def ratios(x):
    res=(float(x.h)/float(x.w),float(x.w)/float(x.h))
    return res

#==============================SORT============
def sort_by_area(rects):
    rects.sort(lambda a,b:cmp(a.area,b.area))
    return rects
    
def sort_by_height(rects):
    rects.sort(lambda a,b:cmp(b.h,a.h))
    return rects
    
def sort_by_width(rects):
    rects.sort(lambda a,b:cmp(b.w,a.w))
    return rects
    
def sort_by_color(rects):
    rects.sort(lambda a,b:cmp(b.color,a.color))
    return rects
    
def sort_by_ratio(rects):
    rects.sort(lambda a,b:cmp(max(ratios(b)),max(ratios(a))))
    return rects

def sort_by_colorsum(rects):
    rects.sort(lambda a,b:cmp(sum(b.color),sum(a.color)))
    return rects

def sort_by_heightratio(rects):
    rects.sort(lambda a,b:cmp(float(b.h)/float(b.w),float(a.h)/float(a.w)))
    return rects

def sort_by_colorpos(n):
    def sort_by_n(rects):
        rects.sort(lambda a,b:cmp(a.color[n],b.color[n]))
        return rects
    return sort_by_n
    

def areas(list):
    return sum([r.w*r.h for r in list])

def one(res):
    return 1

def minpos(pos,val):
    def fcn(res):
        return res[pos]>=val
    return fcn

def maxpos(pos,val):
    def fcn(res):
        return res[pos]<=val
    return fcn

def get_maxpos(tup):
    lst=list(tup)
    mx=max(lst)
    population=lst.count(mx)
    num=random.choice(range(population))
    return lst.index(mx,num)

#~ utp=(4,5,5,5,8,5)
#~ print get_maxpos(utp)

def get_minpos(tup):
    lst=list(tup)
    mn=min(lst)
    population=lst.count(mn)
    return lst.index(mn,random.choice(range(population)))

def tilt(r,settings):
    vec=(0,0)
    if settings.color_tilt:
        cvec=color_tilt(r,settings)
        vec=(vec[0]+cvec[0],vec[1]+cvec[1])
    if settings.shape_tilt:
        svec=shape_tilt(r,settings)
        vec=(vec[0]+svec[0],vec[1]+svec[1])
    return vec

def shape_tilt(r,settings):
    threepts=((-0.57735,0.28867),(0.57735,0.28867),(0,-1))
    if r.w==1:
        dir=threepts[0]
#~         dir=(-0.57735,0.28867)
    elif r.w==3:
        dir=threepts[1]
#~         dir=(-1,0)
    else:
        dir=threepts[2]
#~         dir=(0,0)
#~     if r.w==r.h:
#~         dir=random.choice([(1,0),(-1,0)])
#~     elif r.w>r.h:
#~         dir=(1,0)
#~     else:
#~         dir=(-1,0)
#~     
    vec=(settings.shape_tilt_scale*settings.boardw/2*dir[0],settings.shape_tilt_scale*settings.boardh/2*dir[1])
    vec=ir(vec)
    return vec


def color_tilt(r,settings):
    maxpos=get_maxpos(r.color)
    minpos=get_minpos(r.color)
    if maxpos==0:
        dir=(-0.57735,0.28867*2)
#~         if minpos==1:
#~             dir=(-1*dir[0],-1*dir[1])
        
    if maxpos==1:
        dir=(0.57735,0.28867*2)
#~         if minpos==2:
#~             dir=(-1*dir[0],-1*dir[1])
    if maxpos==2:
        dir=(0,-1+0.28867)
#~         if minpos==0:
#~             dir=(-1*dir[0],-1*dir[1])
    vec=(settings.color_tilt_scale*settings.boardw/2*dir[0],settings.color_tilt_scale*settings.boardh/2*dir[1])
    if settings.color_tilt_by_hue:
        hue=sum(r.color)/(255*3.0)
        vec=(hue*vec[0],hue*vec[1])
    vec=ir(vec)
    return vec
    
def db(lst):
    for l in lst:
        print l,type(l),locals()[0]
