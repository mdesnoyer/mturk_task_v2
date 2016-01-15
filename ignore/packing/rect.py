#MAIN.  adjust settings, then run.  it'll produce a bunch of images, then attempt to animate them.
#rect 2007-10-06

import random,os,math,shutil,time,copy,traceback,types,subprocess,pprint
import Image,ImageFont,ImageDraw
from util import *
from make_anim import anim_dir
try:
    import psyco
    psyco.full()
    pass
except:
    print 'no psyco; will be slower'

def showsettings(settings):
  fields='imgdir old_imgdir ext save_every bgcolor boardw boardh square rectcount create_rect_type color_tilt shape_tilt nosort reverse possible_spots xspot_sort yspot_sort first_sort min_touchcount max_touchcount spot_sort_type'.split()
  for f in fields:
    print f,'=',getattr(settings,f)
    

def get_settings():
    settings=Bag()
    settings.imgdir='images'
    settings.old_imgdir=os.path.join('images','old')
    settings.ext=".png"
    settings.use_bysize=1
    #=========================board
    #========image output
    settings.save_every=10
    settings.bgcolor=(0,0,0)
    settings.bgcolor=(255,255,255)
    #=======board size in pixels
    scaler=2
    settings.boardw,settings.boardh=1280/scaler,1024/scaler
    #=======board scale for output
    settings.scale=scaler
    #=======show descriptive messages
    settings.show_messages=0
    settings.message_width=200
    settings.message_bgcolor=(50,50,50)
    settings.font_size=12

    #========================rectangle generation
    #========general
    settings.square=1
    settings.rectcount=4000

    settings.create_rect_type='uniform'
    #================rect create types:  gauss uniform enum
    #========gauss:  edge length will be abs val of a normal distribution around mu
    settings.mu=5
    settings.stddev=10
    #========uniform distribution:
    settings.rect_maxw=50/scaler
    settings.rect_maxh=50/scaler
    settings.rect_minw=1
    settings.rect_minh=1
    
    settings.rect_size_step=1
    #========enumerate rectangle choices
    
    if settings.create_rect_type=='enum':
        parts=[]
        for n in range(1,80):
            sz=(n,n)
            
            chances=max((80-n)**2/(n),1)
            freq=0
            for _ in range(chances):
                if random.choice(range(10)) in [5,6]:
                    freq+=1
            this=(sz,freq)
            parts.append(this)
        settings.r_enum=[]
        parts=[((x,1),1) for x in range(2,7)]
        parts=[((7,1),1)]
        for size,count in parts:
            for n in range(count):
                settings.r_enum.append(size)
        pprint.pprint(parts)

    

    #================rect colors:
    settings.max_color_sum=255*3*0.95
    settings.min_color_sum=255*3*0.2
    settings.colormins={'red':0,'green':0,'blue':0}
    settings.colormaxes={'red':255,'green':255,'blue':255,}
    settings.area_threshold=0
    #settings.area_functions={(0,3):'darken',(5,20):'lighten',(0,None):'whiten',}
    settings.area_functions={(0,'inf'):'whiten',}
    settings.area_functions={}
    settings.colorsize=0
    settings.color_tilt=0
#~     if settings.color_tilt==1:
#~         settings.use_bysize=0
    settings.color_tilt_by_hue=0
    settings.color_tilt_scale=0.06
    settings.shape_tilt=0
    settings.shape_tilt_scale=0.5

    #=======borders
    settings.use_border=0
    settings.use_ul_border=1
    if settings.use_border or settings.use_ul_border:
        settings.rect_minw=2
        settings.rect_minh=2
    settings.border_color=(255,254,255)

    #================order:
    settings.rect_sort_type='area'
    #========sort_types=area height width color colorsum ratio heightratio red green blue
    settings.nosort=1
    settings.reverse=1

    #================================spots to try rectangle placement
    settings.possible_spots='order'
    #possibe spot types=random middle touching exact-touching corners

    settings.xspot_sort='left'
    settings.yspot_sort='down'
    settings.first_sort='circle'
    settings.min_touchcount,settings.max_touchcount=5,None

    #================================if multiple spots, how to choose from them
    settings.spot_sort_type='first'
    #spot sort types=first closest farthest minfcn maxfcn

    settings.sort_fcns={'area':sort_by_area,
    'height':sort_by_height,
    'width':sort_by_width,
    'color':sort_by_color,
    'colorsum':sort_by_colorsum,
    'heightratio':sort_by_heightratio,
    'ratio':sort_by_ratio,
    'red':sort_by_colorpos(0),
    'green':sort_by_colorpos(1),
    'blue':sort_by_colorpos(2),
    }

    settings.okcolors=[]
    for n,color in [(0,'red'),(1,'green'),(2,'blue'),]:
        settings.okcolors.append(maxpos(n,settings.colormaxes.get(color,255)))
        settings.okcolors.append(minpos(n,settings.colormins.get(color,0)))
    settings.badcolors=[low,high]

    settings.color_functions={'lighten':lighten,
        'darken':darken,'whiten':whiten,
        'blacken':blacken,'equalize':equalize}
    if settings.rectcount%settings.save_every==0:
        settings.rectcount+=1
    
    return settings


def low(res):
    return (sum(res)<=settings.min_color_sum)
    
def high(res):
    return (sum(res)>=settings.max_color_sum)

def clear_space():
    newdir=get_timestring()
    newdir=os.path.join(settings.old_imgdir,newdir)
    print 'moving old images to',newdir
    if not os.path.exists(newdir):
        os.makedirs(newdir)
    
    imgs=os.listdir(settings.imgdir)
    for i in imgs:
        if not (i.endswith(settings.ext) or i.endswith('.jpg') or i.endswith(".gif") or i.endswith(".png")):continue
        ipath=os.path.join(settings.imgdir,i)
        idest=os.path.join(newdir,i)
        shutil.move(ipath,idest)

class Rect(object):
    def __init__(self,ul=None,w=None,h=None,color=(50,50,50)):
        self.ul,self.w,self.h=ul,w,h
        self.area=self.h*self.w
        self.color=color
        self.message=[]
        self.message.append('rect with w,h:%d,%d area %d'%(w,h,self.area))

    def show(self):
        print 'ul:',self.ul,
        print 'w,h:',self.w,self.h
        print 'color',self.color
        print 'area::',self.area
        
class Board(object):
    def __init__(self,w,h):
        self.w,self.h=w,h
        self.center=(int(round(self.w/2)),int(round(self.h/2)))
        print self.w,'x',self.h
        print 'center:',self.center
    
    def show(self):
        print 'w,h:',self.w,self.h
        print 'center:',self.center


    def is_touching(self,target,r):
        res=0
        if settings.min_touchcount==0 and settings.max_touchcount==None:
            return 1
        neighbors=set()
        for xx in range(target[0],target[0]+r.w):
            neighbors.add((xx,target[1]-1))
            neighbors.add((xx,target[1]+r.h))
        for yy in range(target[1],target[1]+r.h):
            neighbors.add((target[0]-1,yy))
            neighbors.add((target[0]+r.w,yy))
        touchcount=0
        for n in neighbors:
            if oob(self.im,n):
                continue
            if touchcount>=settings.min_touchcount and settings.max_touchcount==None:
                res=1
                break
            if im.getpixel(n)!=settings.bgcolor:
                touchcount+=1
        if touchcount>=settings.min_touchcount and (settings.max_touchcount==None or touchcount<=settings.max_touchcount):
            res=1
        return res

def get_choose_from(im):
    print 'getting pts order!'
    st=time.clock()
    w,h=im.size
    xc=range(w)
    yc=range(h)
    choose_from=set()
    if settings.xspot_sort==None:
        random.shuffle(xc)
    if settings.yspot_sort==None:
        random.shuffle(yc)
    if settings.xspot_sort=='right':xc.reverse()
    if settings.yspot_sort=='down':yc.reverse()
    if settings.first_sort=='square':
        mid=(w/2,h/2)
        dir=0
        dist=1
        this=(w/2,h/2)
        for nn in range(w*h):
            dir=(dir+1)%4
            if nn%2==0:
                dist+=1
            for mm in range(dist):
                this=jump(this,dir,1)
                if oob(im,this):
                    break
                obj=(this[0],this[1])
                choose_from.add(obj)
    elif settings.first_sort=='diamond':
        mid=(w/2,h/2)
        dir=0
        dist=1
        this=(w/2,h/2)
        for nn in range(w*h):
            dir=(dir+1)%4
            if nn%2==0:
                dist+=1
            for mm in range(dist):
                this=jump(this,dir,1)
                if oob(im,this):
                    break
                obj=(this[0],this[1])
                choose_from.add(obj)
    elif settings.first_sort=='circle':
        center=(w/2,h/2)
        for xx in range(w):
            for yy in range(h):
                #dist=get_dist(center,(xx,yy))
                obj=(xx,yy)
                choose_from.add(obj)
    
    elif settings.first_sort =='x':
        counter=0
        for xx in xc:
            for yy in yc:
                counter+=1
                obj=(xx,yy)
                choose_from.add(obj)
    elif settings.first_sort =='y':
        counter=0
        for yy in yc:
            for xx in xc:
                counter+=1
                obj=(xx,yy)
                choose_from.add(obj)
    print 'pts order made in: %0.3f'%(time.clock()-st)
    return choose_from

def get_neworder(offcenter):
    order=[]
    if settings.first_sort=='circle':
        for xx in range(int(settings.boardw)):
            if xx%100==0:print xx,
            if xx%500==0:print ''
            for yy in range(settings.boardh):
                dist=get_dist(offcenter,(xx,yy))
                obj=(xx,yy)
                order.append((dist,obj))
#~         random.shuffle(order)
        order.sort()
        order=[x[1] for x in order]
    return order

def make_message_im(r):
    msgbox_size=(settings.message_width,settings.boardh)
    im=Image.new('RGB',msgbox_size,settings.message_bgcolor)
    font=ImageFont.truetype('arial.ttf',settings.font_size)
    draw=ImageDraw.Draw(im)
    for ii,l in enumerate(r.message):
        text=str(ii)+":"+l
        spot=(0,ii*settings.font_size)
        draw.text(spot,text,font=font)
    return im

class Sol(object):
    def __init__(self,board,rects):
        self.board=board
        self.rects=rects
        self.im=Image.new('RGB',(self.board.w,self.board.h),settings.bgcolor)
        self.filled={}
        
    def fits(self,xx,yy,r):
        good=(xx,yy)
        done=0
        if r.w+xx>self.board.w or r.h+yy>self.board.h:
            #print 'cant fit'
            good=None
            done=1
            method='overlaps edge'
        if done:
            return good,method
        semicorners=[(xx,yy),(xx+r.w-1,yy),(xx+r.w-1,yy+r.h-1),(xx,yy+r.h-1),(xx+r.w/2,yy+r.h/2),]
        if r.w>1 and r.h>2:
            semicorners.extend([(xx+1,yy+1),(xx+r.w-2,yy+1),(xx+1,yy+r.h-2),(xx+r.w-2,yy+r.h-2)])
        for corner in semicorners:
            if self.im.getpixel(corner) != settings.bgcolor:
                #print 'bad corner'
                done=1
                good=None
                method='semicorner bad:'+str(corner)
                break
        if done:
            return good,method
        for rx in range(r.w):
            for ry in range(r.h):
                target=(xx+rx,yy+ry)
                if self.im.getpixel(target) != settings.bgcolor:
                    #print 'bad spot'
                    done=1
                    good=None
                    method='bad spot at:'+str(target)
                    break
                if done:return good,method
        method='made it through!!!'
        return good,method

    def find_space(self,r):
        findst=time.clock()
        good=None
        skipped=set()
        max=self.board.w*self.board.h
        tries=0
        r.ul=None
#~         choose_from=self.choose_from
        removed=set()
        whilest=time.clock()
        center=(int(round(settings.boardw/2)),int(round(settings.boardh/2)))
        st=time.clock()
        xtilt=(-1*r.w/2)
        ytilt=(-1*r.h/2)
#~         print 'x,ytilt:',xtilt,ytilt
        
        xmod,ymod=tilt(r,settings)
        xtilt+=xmod
        ytilt+=ymod
        
        xtilt=ir(xtilt)
        ytilt=ir(ytilt)
        
        this_center=(xtilt,ytilt)
        
        
        sz=(r.w,r.h,this_center)
        bysize=settings.rect_progressions.get(sz,0)
        for ii,m in enumerate(settings.circle_order):
            
            if settings.use_bysize and ii<bysize:
                continue

            loopst=time.clock()
            xx,yy=(m[0]+this_center[0],m[1]+this_center[1])
            target=xx,yy
            if oob(self.im,target):
                continue
            res=self.im.getpixel(target)
            tries+=1
            if res!=settings.bgcolor:
#~                 print 'full'
                continue
            
            st=time.clock()
            good,method=self.fits(xx,yy,r)
            if good==None:
#~                 print 'not good, method:',method
                continue
            else:
#~                 print 'tries:',tries
#~                 r.message.append('distance was: %0.3f'%(get_dist(self.board.center,target)))
#~                 print 'tries:',tries,
                settings.rect_progressions[sz]=ii
                break
                
        st=time.clock()
        if good==None:
            pass
#~             print 'couldnt find spot for',r.w,'x',r.h,'area is:',r.area
#~             r.message.append('could not find spot at all')

        else:
#~             print 'found spot!',good,'for',r.w,'x',r.h,'in %0.4f'%(time.clock()-findst),'area is:',r.area
            st=time.clock()
            r.ul=good
            #print self.choose_from.spots
        #print 'all find space took:%0.5f'%(time.clock()-findst)

    

    def addrect(self,r):
        if r.ul==None:
#~             print 'could not add rect'
            return None
        if settings.use_border:
            bordim=Image.new('RGB',(r.w,r.h),settings.border_color)
            self.im.paste(bordim,r.ul)
            innersize=(r.w-2,r.h-2)
            if innersize[0]<1 or innersize[1]<1:
                return None
            rim=Image.new('RGB',innersize,r.color)
            self.im.paste(rim,(r.ul[0]+1,r.ul[1]+1))
        if settings.use_ul_border:
            bordim=Image.new('RGB',(r.w,r.h),settings.border_color)
            self.im.paste(bordim,r.ul)
            innersize=(r.w-1,r.h-1)
            if innersize[0]<1 or innersize[1]<1:
                return None
            rim=Image.new('RGB',innersize,r.color)
            self.im.paste(rim,(r.ul[0]+1,r.ul[1]+1))
        else:
            rim=Image.new('RGB',(r.w,r.h),r.color)
            self.im.paste(rim,r.ul)
        return 1

    def solve(self):
        clear_space()
        lastarea=None
        res=None
        st=time.clock()
        tot=len(self.rects)
        saved=0
        for ii,r in enumerate(self.rects):
            if ii%100==1:
                per=(time.clock()-st)/ii
                print ii,"per:%0.4f"%per
            rectst=time.clock()
            if ii==0:r.first=1
            else:r.first=0
            self.find_space(r)
            res=self.addrect(r)
            if res==None:
                pass
            else:
                saved=0
            if saved==0 and ii%settings.save_every==0:
                fn=os.path.join(settings.imgdir,"%05d"%ii+settings.ext)
                print 's',
                temp=self.im.resize(((self.im.size[0]*settings.scale),(self.im.size[1]*settings.scale)))
                if settings.show_messages:
                    msgim=make_message_im(r)
#~                         print 'made message im:',msgim
                    temp.paste(msgim,(settings.boardw*settings.scale,0))
                temp.save(fn)
                saved=1
            lastarea=r.area
        fn=os.path.join(settings.imgdir,"%05d"%ii+settings.ext)
        temp.save(fn)
            #print 'whole rect took %0.5f'%(time.clock()-rectst)



def get_randcolor(size):
    w,h=size
    area=w*h
    while 1:
        good=1
        res=(int(random.randrange(settings.colormins['red'],settings.colormaxes['red'])),
            int(random.randrange(settings.colormins['green'],settings.colormaxes['green'])),
            int(random.randrange(settings.colormins['blue'],settings.colormaxes['blue'])))

        for fcn in settings.okcolors:
            if not fcn(res):
                good=0
                break
        if not good:
            continue
        for fcn in settings.badcolors:
            if fcn(res):
                good=0
                break
        if settings.colorsize:
            res.sort()
            if w>h:
                res.reverse()
        if settings.area_threshold:
            for arearange,fcnname in settings.area_functions.items():
                fcn=settings.color_functions[fcnname]
                bot,top=arearange
                if area>bot and (area<top or top=='inf'):
                    res=fcn(res)
        
        if not good:
            continue
        if good:
            break
    return res
    
def makerects():
    rects=[]
    while len(rects)<settings.rectcount:
        if settings.create_rect_type=='gauss':
            w=int(abs(random.gauss(settings.mu,settings.stddev))+1)
            h=int(abs(random.gauss(settings.mu,settings.stddev))+1)
        if settings.create_rect_type=='uniform':
            w=random.randrange(settings.rect_minw,settings.rect_maxw+1,settings.rect_size_step)
            h=random.randrange(settings.rect_minh,settings.rect_maxh+1,settings.rect_size_step)
        if settings.create_rect_type=='enum':
            w,h=random.choice(settings.r_enum)
        if settings.square:
            w=h
        if w<settings.rect_minh or h<settings.rect_minh:
            continue
        randcolor=get_randcolor((w,h))
        thisrect=Rect(ul=None,w=w,h=h,color=randcolor)
        rects.append(thisrect)
    sort_fcn=settings.sort_fcns[settings.rect_sort_type]
    if not settings.nosort:
        rects=sort_fcn(rects)
        print 'rects sorted according to:',settings.rect_sort_type,'!'
    if settings.reverse:
        rects.reverse()
        print 'rects have been reversed'
    return rects

def run():
    global settings
    settings=get_settings()
    offcenter=(settings.boardw/2,settings.boardh/2)
    settings.rect_progressions={}
    settings.circle_order=get_neworder(offcenter)
    rects=makerects()
    print 'made',len(rects),'rects'
    board=Board(settings.boardw,settings.boardh)
    s=Sol(board,rects)
    s.solve()

if __name__=="__main__":
    
    global settings
    settings=get_settings()
    showsettings(settings)
    offcenter=(settings.boardw/2,settings.boardh/2)
    settings.rect_progressions={}
    print 'making circle order:',
    settings.circle_order=get_neworder(offcenter)
    print 'made'
    print 'making rects',
    rects=makerects()
    print 'made',len(rects),'rects'
    board=Board(settings.boardw,settings.boardh)
    s=Sol(board,rects)
    s.solve()
    anim_dir('images')
