#include "config.h"
#include <stdio.h>
#include <unistd.h>
#ifdef HAVE_MACH_THREAD_POLICY_H
#include <mach/mach_init.h>
#include <mach/thread_policy.h>
// #include <mach/sched.h>
#endif
#ifdef HAVE_DECL_SCHED_SETAFFINITY
//#define _GNU_SOURCE             /* See feature_test_macros(7) */
#include <sched.h>
#endif
#include <errno.h>
#include <caml/mlvalues.h>

CAMLprim value numcores(value unit) {
  int numcores = sysconf( _SC_NPROCESSORS_ONLN );
  return Val_int(numcores);
}

CAMLprim value setcore(value which) {
  int numcores = sysconf( _SC_NPROCESSORS_ONLN );
  int w = Int_val(which) % numcores; // stay in the space of existing cores
#ifdef HAVE_DECL_SCHED_SETAFFINITY
  cpu_set_t cpus;   
#endif
#ifdef HAVE_MACH_THREAD_POLICY_H
  thread_affinity_policy_data_t affinityData;
#endif
  int retcode;
  int finished=0;
  while (finished==0)
    {
#ifdef HAVE_DECL_SCHED_SETAFFINITY
      CPU_ZERO(&cpus); 
      CPU_SET (w,&cpus);
      //fprintf(stderr,"Trying to pin to cpu %d out of %d reported by the system\n",w,numcores);
      retcode = sched_setaffinity(getpid(), sizeof(cpu_set_t), &cpus);
      if(retcode != 0) {
	fprintf(stderr,"Failed pinning to cpu %d, trying %d/2\n",w, w); 
	w=w/2;
      }
      else
#endif
#ifdef HAVE_MACH_THREAD_POLICY_H
      affinityData.affinity_tag = w;
      retcode = thread_policy_set(mach_thread_self(),
                        THREAD_AFFINITY_POLICY,
                        &affinityData,
                        THREAD_AFFINITY_POLICY_COUNT);
      if(retcode) {
        fprintf(stderr,"MAC OS X: Failed pinning to cpu %d, trying %d/2\n",w, w);
        w=w/2;
      }
      else 
#endif
	{ //fprintf(stderr,"Succeeded pinning to cpu %d\n",w); 
	  finished=1;
	}
    }
  return Val_unit;
}
