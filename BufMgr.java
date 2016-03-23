package bufmgr;

import global.GlobalConst;
import global.Minibase;
import global.Page;
import global.PageId;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

import diskmgr.*;

/**
 * <h3>Minibase Buffer Manager</h3>
 * The buffer manager reads disk pages into a main memory page as needed. The
 * collection of main memory pages (called frames) used by the buffer manager
 * for this purpose is called the buffer pool. This is just an array of Page
 * objects. The buffer manager is used by access methods, heap files, and
 * relational operators to read, write, allocate, and de-allocate pages.
 */
public class BufMgr implements GlobalConst {

    /** Actual pool of pages (can be viewed as an array of byte arrays). */
    protected Page[] bufpool;

    /** Array of descriptors, each containing the pin count, dirty status, etc\
	. */
    protected FrameDesc[] frametab;

    /** Maps current page numbers to frames; used for efficient lookups. */
    protected HashMap<Integer, FrameDesc> pagemap;

    /** The replacement policy to use. */
    protected Replacer replacer;
    
    
//-------------------------------------------------------------



  /**
   * Constructs a buffer manager with the given settings.
   * 
   * @param numbufs number of buffers in the buffer pool
   */
  public BufMgr(int numbufs) 
  {
	  //initialise all properties
   bufpool=new Page[numbufs];
   frametab=new FrameDesc[numbufs];
   for(int i=0;i<numbufs;i++)
   {
	   bufpool[i]=new Page();      
	   frametab[i]=new FrameDesc(i);
   }
   
   pagemap=new HashMap<Integer,FrameDesc>(numbufs);
   
   // pass the reference to clock to ensure single copy of properties is maintained  
   replacer=new Clock(this);
   
  }
  
 


/**
   * Allocates a set of new pages, and pins the first one in an appropriate
   * frame in the buffer pool.
   * 
   * @param firstpg holds the contents of the first page
   * @param run_size number of pages to allocate
   * @return page id of the first new page
   * @throws IllegalArgumentException if PIN_MEMCPY and the page is pinned
   * @throws IllegalStateException if all pages are pinned (i.e. pool exceeded)
   */

  public PageId newPage(Page firstpg, int run_size)
  {
		
	  PageId pgID=new PageId();
	
	
		pgID=Minibase.DiskManager.allocate_page(run_size);
	//	System.out.println("new page id allocated: "+pgID.pid);
	
			try
	        {
				
			  pinPage(pgID, firstpg, true);
	        }
	        catch(IllegalStateException e)
	        {
	        	//System.out.println(" : ) ");
	            for(int j = 0; j < run_size; j++)
	            {
	                j=pgID.pid+j;
	                Minibase.DiskManager.deallocate_page(pgID);       //if allocation fails deallocate all the pages requested in run_size
	            }

	            throw new IllegalStateException("pin exceeded");
	        }

		
		  
		
		
		
			//System.out.println("made new page: with pg id" +pagemap.get(pgID.pid).getPageno());
	
			return pgID;
	
	  
  }

  /**
   * Deallocates a single page from disk, freeing it from the pool if needed.
   * 
   * @param pageno identifies the page to remove
   * @throws IllegalArgumentException if the page is pinned
   */
  public void freePage(PageId pageno) 
  {

		
	        
	        FrameDesc fd=(FrameDesc)pagemap.get(Integer.valueOf(pageno.pid));
	        if(fd!=null)
	        {
	        	if(fd.pincnt>0)
	        		  throw new IllegalArgumentException("Page currently pinned");
	        	pagemap.remove(Integer.valueOf(pageno.pid));      
	        	fd.pageno.pid=-1;
	        	fd.pincnt=0;
	        	fd.dirty=false;
	        	fd.state=10;                 // remove the page from bufpool and make the frame available
	        }
	        Minibase.DiskManager.deallocate_page(pageno);
	
  }

	  
  

  /**
   * Pins a disk page into the buffer pool. If the page is already pinned, this
   * simply increments the pin count. Otherwise, this selects another page in
   * the pool to replace, flushing it to disk if dirty.
   * 
   * @param pageno identifies the page to pin
   * @param page holds contents of the page, either an input or output param
   * @param skipRead PIN_MEMCPY (replace in pool); PIN_DISKIO (read the page in)
   * @throws IllegalArgumentException if PIN_MEMCPY and the page is pinned
   * @throws IllegalStateException if all pages are pinned (i.e. pool exceeded)
   */
  public void pinPage(PageId pageno, Page page, boolean skipRead) 
  {
	 // System.out.println("Page id received for pinning"+ pageno.pid);
	 
	  FrameDesc fd;
	 fd=(FrameDesc)pagemap.get(Integer.valueOf(pageno.pid));
		    if(fd!=null)  // page is in bufpool
		    if(skipRead)
	            {
		    	// page is currently in bufpool and pinned and cannot directly returned 
	                throw new IllegalArgumentException("Page pinned; PIN_MEMCPY not allowed");
	            } else
	            {
	              
	                fd.pincnt++;
	               
	                fd.state=12;  //pinned
	                
	                page.setPage(bufpool[fd.index]);
	                return;
	            }
		    int victim=replacer.pickVictim(); 
		    
		  //  System.out.println("********vitim*****"+victim);
		    if(victim<0)
		   throw new IllegalStateException("Buffer pool exceeded");
		    fd=frametab[victim];
		    if(fd.pageno.pid !=-1)
		    {
		    	pagemap.remove(Integer.valueOf(fd.pageno.pid));
		    	if(fd.dirty)
		    		 Minibase.DiskManager.write_page(fd.pageno, bufpool[victim]);
		    	}
		    if(skipRead)
		    	bufpool[victim].copyPage(page); // if page not in bufpool it has to be in memory
		    else
		    	Minibase.DiskManager.read_page(pageno,bufpool[victim]);  // read the page from disk as page is not in memory nor in bufpool
		    page.setPage(bufpool[victim]);  //set the page just to be double sure.
		    
		
		    fd.pageno.pid=pageno.pid;
		    fd.pincnt=1;
		    fd.dirty=false;
		    fd.state=12;    // mark it as pinned and return
		    pagemap.put(Integer.valueOf(pageno.pid), fd); //update in pagemap.
		  
		   
		    	
	

}

  /**
   * Unpins a disk page from the buffer pool, decreasing its pin count.
   * 
   * @param pageno identifies the page to unpin
   * @param dirty UNPIN_DIRTY if the page was modified, UNPIN_CLEAN otherrwise
   * @throws IllegalArgumentException if the page is not present or not pinned
   */
  public void unpinPage(PageId pageno, boolean dirty) 
  {
	  
	  FrameDesc fd;
	  fd=(FrameDesc)pagemap.get(Integer.valueOf(pageno.pid));
       if(fd == null)
           throw new IllegalArgumentException("Page not present");
      if(fd.pincnt == 0)
       {
           throw new IllegalArgumentException("Page not pinned");
       } else
       {
    	   
    	   fd.pincnt--;  // reduce the pin count for un pining
    	  
    	   if(dirty)
    		   fd.dirty=dirty;    // if caller says the page is dirty only then park it dirty else unchanged
    	   if(fd.pincnt==0)
    	   fd.state=11; // mark it referenced as it is recently used.
    	   return;
       }
	  	
  }

  /**
   * Immediately writes a page in the buffer pool to disk, if dirty.
   */
  public void flushPage(PageId pageno) 
  { 
	  
  
	  FrameDesc fd=(FrameDesc)pagemap.get(pageno.pid);
	  if(fd==null)
		  throw new IllegalArgumentException("Page not present");   //if page cannot be found in pagemap throw an error
      if(fd.dirty) // flush the pages which are dirty
      {
          Minibase.DiskManager.write_page(fd.pageno, bufpool[fd.index]);
          frametab[fd.index].dirty = false;  // after flushing revert the bits
      }
  }

  /**
   * Immediately writes all dirty pages in the buffer pool to disk.
   */
  public void flushAllPages() 
  {
	  System.out.println("*******flush all pages called********");

	  
	
	  
	  for(int i = 0; i < frametab.length; i++)
          if( frametab[i].dirty)    // flush the pages which are dirty
          {
              Minibase.DiskManager.write_page(frametab[i].pageno, bufpool[i]);
              frametab[i].dirty = false;        // after flushing revert the bits
          }
	  
  }
  


  /**
   * Gets the total number of buffer frames.
   */
  public int getNumBuffers() {
	
	  
	  return bufpool.length; // simply revert the size of pool
  }

  /**
   * Gets the total number of unpinned buffer frames.
   */
  public int getNumUnpinned() 
  {
	  int i = 0;
      for(int j = 0; j < frametab.length; j++)   //traverse to count the dirty pages.
          if(frametab[j].pincnt == 0)
              i++;

      return i;
	 
	    
  }
/*  public void getvicitm()
  {
	  int newframe=replacer.pickVictim();
	  System.out.println("frame "+newframe);
  }*/

} // public class BufMgr implements GlobalConst
