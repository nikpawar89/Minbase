package bufmgr;

import java.util.Map;

import global.Minibase;

/**
 * The "Clock" replacement policy.
 */
class Clock extends Replacer {

  //
  // Frame State Constants
  //
  protected static final int AVAILABLE = 10;
  protected static final int REFERENCED = 11;
  protected static final int PINNED = 12;
 public static int victim=0;
  /** Clock head; required for the default clock algorithm. */
  protected int head;

  // --------------------------------------------------------------------------

  /**
   * Constructs a clock replacer.
   */
  public Clock(BufMgr bufmgr) {
    super(bufmgr);

    // initialize the frame states
    for (int i = 0; i < frametab.length; i++) {
      frametab[i].state = AVAILABLE;
     
    }

    // initialize the clock head
    head = -1;

  } // public Clock(BufMgr bufmgr)

  /**
   * Notifies the replacer of a new page.
   */
  public void newPage(FrameDesc fdesc) {
    // no need to update frame state
	  
  }

  /**
   * Notifies the replacer of a free page.
   */
  public void freePage(FrameDesc fdesc) {
	  
    
  }

  /**
   * Notifies the replacer of a pined page.
   */
  public void pinPage(FrameDesc fdesc) 
  {
	 
	  
	  
  }

  /**
   * Notifies the replacer of an unpinned page.
   */
  public void unpinPage(FrameDesc fdesc) 
  {
    
    	 
    	
          
	  
	  
	
  }   

  /**
   * Selects the best frame to use for pinning a new page.
   * 
   * @return victim frame number, or -1 if none available
   */
  public int pickVictim() 
  {
	
	
      for(int j=0;j<frametab.length;j++)
      {
    	  head=(head+1)%frametab.length;
    	  if(frametab[head].state == REFERENCED && (frametab[head].pincnt==0))
    	  {
    		  frametab[head].state = AVAILABLE;  // change reference to available for second chance and continue
    		  continue;
    	  }
    	  if(frametab[head].state==AVAILABLE && (frametab[head].pincnt==0))
    		  break;  // if you find available frame, use it as 
    		  
      }
      return head;

    // keep track of the number of tries

    // return the victim page
       //for compilation

  } // public int pick_victim()

} // class Clock extends Replacer
