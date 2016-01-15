#one by one convert
#makes chunks, then makes those into animations too.
#requires imagemagick, which kind of sucks at making animated gifs.

import os,time,random
import subprocess





def is_pic(p):
    #print 'checking is pic for',p
    if p.endswith('.gif') or p.endswith('.png') or p.endswith('.jpg'):
        return 1
    return 0

def pairify(ps):
    if len(ps)>1:
        thispair=[ps.pop(0),ps.pop(0)]
        

def is_tempfile(p):
    if p.startswith('temp-'):
        return 1
    return 0
    
def get_temppath(dir):
    global g_res_ext
    while 1:
        randstr='temp-'+''.join([chr(random.randrange(97,123)) for x in range(10)])+g_res_ext
        temppath=os.path.join(dir,randstr)
        if not os.path.exists(temppath):
            break
    return temppath

def del_tempfiles(dir):
    ps=os.listdir(dir)
    print 'in del temp for dir',dir
    print 'got ps:,',ps
    global batfile
    badfs=dir+"\\temp*"+g_res_ext
    cmd='del '+badfs
    print 'wronte command:',cmd
    batfile.write(cmd+'\n')


        
def put_together(ps,dir):
    temppath=get_temppath(dir)
    cmd='convert '+" ".join(ps)+' '+temppath
    batfile.write(cmd+"\n")
    print 'wrote command:',cmd
    return temppath

def makechunks(ps):
    global g_chunksize
    chunks=[]
    if len(ps)%g_chunksize!=0:
        c1=[ps.pop(0) for n in range((len(ps)%g_chunksize))]
        chunks=[c1]
    while len(ps):
        
        thischunk=[ps.pop(0) for _ in range(g_chunksize)]
        chunks.append(thischunk)
    print 'chunks made:',chunks
    return chunks

def dops(ps,dir):
    #print 'doing ps:',ps
    while len(ps)>g_chunksize:
        cmd='echo '+str(len(ps))
        batfile.write(cmd+"\n")
        print 'cmd is:',cmd
        chunks=makechunks(ps)
        ps=[dops(chunk,dir) for chunk in chunks]
        print 'ps is now:',ps
    res=put_together(ps,dir)
    return res

def anim_dir(dir):
    global g_chunksize
    g_chunksize=3
    global batfile,final_filename,g_src_ext,g_res_ext
    g_src_ext='.png'
    g_res_ext='.gif'
    from util import get_timestring
    final_filename=dir+'-final-'+get_timestring()+g_res_ext
    print 'final filename is:',final_filename
    batfile=open(dir+'-temp.bat','w')
    print 'dir is:',dir
    files=os.listdir(dir)
    g_skip=1
    ps=[]
    for ii,p in enumerate(files):
        ppath=os.path.join(dir,p)
        if not is_pic(ppath):
            continue
        if ii%g_skip!=0:
            continue
        ps.append(ppath)
    print 'got ps:',ps
    ps=[dops(ps,dir)]
    
    global final_filename
    print 'done! copying to final filename:',final_filename
    cmd='copy '+ps[0]+' '+final_filename
    print 'wrote command:',cmd
    batfile.write(cmd+"\n")
    print 'deleting temp files!'
    del_tempfiles(dir)
    cmd='pause'
    print 'cmd is:',cmd
    batfile.write(cmd+"\n")
    batfile.close()
    
if __name__=="__main__":
    dir=os.path.join('images','old','2007-10-15-14-09-50')
    dir='images'
    anim_dir(dir)