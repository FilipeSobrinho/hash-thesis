// serial_CM_r2.cpp
#include <algorithm>
#include <array>
#include <cstdint>
#include <cstring>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <string>
#include <unordered_map>
#include <vector>
#include "sketch/countmin.hpp"
#include "core/randomgen.hpp"
#include "hash/msvec.hpp"
#include "hash/simpletab32.hpp"
#include "hash/tornado32.hpp"
#include "hash/rapidhash.h"
#include "core/r2.hpp"
struct View{ const uint8_t* p; uint32_t len; bool operator==(const View&o)const noexcept{ return len==o.len && std::memcmp(p,o.p,len)==0; } };
struct ViewHash{ size_t operator()(const View&v)const noexcept{ uint32_t h=2166136261u; for(uint32_t i=0;i<v.len;++i){ h^=v.p[i]; h*=16777619u;} return (size_t)h; } };
struct RowHash32{ uint32_t a=0,b=0; uint32_t hash(uint32_t x)const{ return a*x+b; } static RowHash32 random(){ RowHash32 r; r.a=(uint32_t)rng::get_u64()|1u; r.b=(uint32_t)rng::get_u64(); return r; } };
int main(int,char**){
  size_t WIDTH=32768,DEPTH=3,R=50000; std::string outfile="cms_r2_relerr.csv";
  std::ofstream out(outfile,std::ios::binary); if(!out){ std::cerr<<"Cannot open "<<outfile<<"\n"; return 1;} out.setf(std::ios::fixed); out<<std::setprecision(8); out<<"function,rep,relerr\n";
  datasets::R2 base; const auto& buf=base.buffer(); const auto& index=base.index(); const size_t N=index.size(); if(!N){ std::cerr<<"R2 empty\n"; return 2; }
  std::unordered_map<View,uint32_t,ViewHash> freq; freq.reserve(N); for(size_t i=0;i<N;++i){ const auto [off,len]=index[i]; ++freq[View{buf.data()+off,(uint32_t)len}]; }
  std::vector<View> distinct; distinct.reserve(freq.size()); for(auto&kv:freq) distinct.push_back(kv.first);
  enum{IDX_MSVEC,IDX_TABMS,IDX_T1,IDX_T2,IDX_T3,IDX_T4,IDX_RAPID,NUM}; const char* NAMES[NUM]={"MSVec","TabOnMSVec","TornadoOnMSVecD1","TornadoOnMSVecD2","TornadoOnMSVecD3","TornadoOnMSVecD4","RapidHash32"};
  using Coeffs=std::array<uint64_t,MSVEC_NUM_COEFFS>; struct P{ Coeffs c; uint64_t seed; std::vector<RowHash32> rows; };
  std::vector<P> V(R); for(size_t r=0;r<R;++r){ for(size_t i=0;i<MSVEC_NUM_COEFFS;++i)V[r].c[i]=rng::get_u64(); V[r].seed=rng::get_u64(); V[r].rows.resize(DEPTH); for(size_t d=0;d<DEPTH;++d) V[r].rows[d]=RowHash32::random(); }
  auto mre=[&](const sketch::CountMin& cms, auto&& h32)->double{ long double s=0; for(const auto& v:distinct){ uint32_t t=freq.find(v)->second; uint32_t est=cms.estimate(h32(v.p,v.len)); s += ((long double)est - (long double)t) / (long double)t; } return (double)(s/(long double)distinct.size()); };
  for(size_t r=0;r<R;++r){
    hashfn::MSVec ms; ms.set_params(V[r].c,true);
    hashfn::TabOnMSVec tab; tab.set_params(V[r].c,true);
    hashfn::TornadoOnMSVecD1 t1; t1.set_params(V[r].c,true);
    hashfn::TornadoOnMSVecD2 t2; t2.set_params(V[r].c,true);
    hashfn::TornadoOnMSVecD3 t3; t3.set_params(V[r].c,true);
    hashfn::TornadoOnMSVecD4 t4; t4.set_params(V[r].c,true);
    rapid::RapidHash32 rh; rh.set_params(V[r].seed, rapid_secret[0], rapid_secret[1], rapid_secret[2]);
    auto fam=[&](int f,const void*p,size_t len)->uint32_t{ switch(f){ case IDX_MSVEC: return ms.hash(p,len); case IDX_TABMS: return tab.hash(p,len);
      case IDX_T1: return t1.hash(p,len); case IDX_T2: return t2.hash(p,len); case IDX_T3: return t3.hash(p,len); case IDX_T4: return t4.hash(p,len);
      case IDX_RAPID: return rh.hash(p,len); default: return 0; } };
    for(int f=0;f<NUM;++f){ sketch::CountMin cms(WIDTH,DEPTH); std::vector<RowHash32> rows=V[r].rows; for(size_t d=0;d<DEPTH;++d) cms.set_row(d,&rows[d]);
      for(size_t i=0;i<N;++i){ const auto [off,len]=index[i]; const void* p=buf.data()+off; cms.add(fam(f,p,len),1); } out<<NAMES[f]<<","<<(r+1)<<","<<mre(cms,[&](const void*p,size_t l){return fam(f,p,l);})<<"\n"; }
  } std::cout<<"Done.\n"; return 0;
}