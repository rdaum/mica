/** Copyright (C) Ryan Daum 2001, 2002, 2003.  See COPYING for details.
*/
#include "common/mica.h"

#include "Timer.hh"

timeval mica::timer_sub(timeval t1, timeval t2)
{
    t1.tv_sec -= t2.tv_sec;
    t1.tv_usec -= t2.tv_usec;
    if (t1.tv_usec < 0) {
        t1.tv_usec += 1000000;
        t1.tv_sec--;
    }
    return t1;
}

timeval
mica::timer_addmsec(timeval t, int msec)
{
    t.tv_sec += msec / 1000;
    t.tv_usec += (msec % 1000) * 1000;
    if (t.tv_usec > 1000000) {
        t.tv_usec -= 1000000;
        t.tv_sec++;
    }
    return t;
}

