/** Copyright (C) Ryan Daum 2001, 2002, 2003.  See COPYING for details.
 */
#ifndef TIMER_HH
#define TIMER_HH

#ifdef _WIN32
#include <time.h>
#include <winsock.h>
#else
#include <sys/time.h>
#endif


namespace mica {

  class Timer
  {
  public:

    bool started;
    struct timeval _start, _end;

    Timer() {
      started = false;
    }

    ~Timer() {}

    /** [Re]starts this timer.
     */
    inline void reset() {
      started = true;
      gettimeofday( &_start, 0 );
    }

    /** @return how many microseconds have elapsed on this timer
     */
    inline unsigned long status() {
      gettimeofday( &_end, 0 );
      unsigned long lapsed;
      lapsed =((_end.tv_sec - _start.tv_sec)* 1000000) + 
	(_end.tv_usec - _start.tv_usec);
      return lapsed;
    }

  }; 
  extern timeval timer_sub(timeval t1, timeval t2);
  extern timeval timer_addmsec(timeval t, int msec);
};

#endif
