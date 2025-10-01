// serial_OPH_a1.cpp
#include <algorithm>
#include <cstdint>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <string>
#include <unordered_set>
#include <vector>
#include "sketch/oph.hpp"
#include "core/randomgen.hpp"
#include "hash/ms.hpp"
#include "hash/simpletab32.hpp"
#include "hash/tornado32.hpp"
#include "hash/rapidhash.h"
#include "core/a1.hpp"
static inline uint32_t load_le_u32(const void* p){ const uint8_t* b=(const uint8_t*)p; return (uint32_t)b[0]|(uint32_t(b[1])<<8)|(uint32_t(b[2])<<16)|(uint32_t(b[3])<<24); }
static double jtrue(const std::vector<uint32_t>&A,const std::vector<uint32_t>&B){
  std::unordered_set<uint32_t> As; As.reserve(A.size()); std::unordered_set<uint32_t> Bs; Bs.reserve(B.size());
  for(auto k:A) As.insert(k); for(auto k:B) Bs.insert(k); size_t inter=0;
  if(As.size()<Bs.size()) for(auto k:As) if(Bs.count(k)) ++inter; else;
  else for(auto k:Bs) if(As.count(k)) ++inter;
  size_t uni=As.size()+Bs.size()-inter; return uni? double(inter)/double(uni):1.0;
}
int main(int,char**){
  size_t ITEMS=500000,K=200,R=50000; uint64_t split_seed=0xC0FFEEull; std::string outfile="oph_a1_relerr.csv";
  std::ofstream out(outfile,std::ios::binary); if(!out){ std::cerr<<"Cannot open "<<outfile<<"\n"; return 1;} out.setf(std::ios::fixed); out<<std::setprecision(8); out<<"function,rep,relerr\n";
  datasets::A1Split split(ITEMS,split_seed);
  std::vector<uint32_t>A,B; { auto s=split.make_streamA(); const void* p; size_t l; while(s.next(p,l)) A.push_back(load_le_u32(p)); }
  { auto s=split.make_streamB(); const void* p; size_t l; while(s.next(p,l)) B.push_back(load_le_u32(p)); }
  const double JT=jtrue(A,B); const double denom=(JT>0.0?JT:1.0);
  enum{IDX_MS,IDX_STAB,IDX_T1,IDX_T2,IDX_T3,IDX_T4,IDX_RAPID,NUM}; const char* NAMES[NUM]={"MultShift","SimpleTab","TornadoD1","TornadoD2","TornadoD3","TornadoD4","RapidHash32"};
  struct P{ uint64_t a,b,s; }; std::vector<P> V(R); for(size_t r=0;r<R;++r) V[r]={rng::get_u64(),rng::get_u64(),rng::get_u64()};
  for(size_t r=0;r<R;++r){
    hashfn::MS ms; ms.set_params(V[r].a,V[r].b);
    hashfn::SimpleTab32 tab; tab.set_params();
    hashfn::TornadoTab32D1 t1; t1.set_params();
    hashfn::TornadoTab32D2 t2; t2.set_params();
    hashfn::TornadoTab32D3 t3; t3.set_params();
    hashfn::TornadoTab32D4 t4; t4.set_params();
    rapid::RapidHash32 rh; rh.set_params(V[r].s, rapid_secret[0], rapid_secret[1], rapid_secret[2]);
    sketch::OPH SA[NUM]={sketch::OPH((uint32_t)K),sketch::OPH((uint32_t)K),sketch::OPH((uint32_t)K),sketch::OPH((uint32_t)K),sketch::OPH((uint32_t)K),sketch::OPH((uint32_t)K),sketch::OPH((uint32_t)K)};
    sketch::OPH SB[NUM]={sketch::OPH((uint32_t)K),sketch::OPH((uint32_t)K),sketch::OPH((uint32_t)K),sketch::OPH((uint32_t)K),sketch::OPH((uint32_t)K),sketch::OPH((uint32_t)K),sketch::OPH((uint32_t)K)};
    for(auto k:A){ SA[IDX_MS].push(ms.hash(k)); SA[IDX_STAB].push(tab.hash(k)); SA[IDX_T1].push(t1.hash(k)); SA[IDX_T2].push(t2.hash(k)); SA[IDX_T3].push(t3.hash(k)); SA[IDX_T4].push(t4.hash(k)); SA[IDX_RAPID].push(rh.hash(&k,sizeof(k))); }
    for(auto k:B){ SB[IDX_MS].push(ms.hash(k)); SB[IDX_STAB].push(tab.hash(k)); SB[IDX_T1].push(t1.hash(k)); SB[IDX_T2].push(t2.hash(k)); SB[IDX_T3].push(t3.hash(k)); SB[IDX_T4].push(t4.hash(k)); SB[IDX_RAPID].push(rh.hash(&k,sizeof(k))); }
    for(int f=0;f<NUM;++f){ double Jest=sketch::jaccard(SA[f],SB[f]); out<<NAMES[f]<<","<<(r+1)<<","<<((Jest-JT)/denom)<<"\n"; }
  } std::cout<<"Done.\n"; return 0;
}