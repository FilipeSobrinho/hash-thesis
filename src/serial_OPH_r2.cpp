// serial_OPH_r2.cpp
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
#include "hash/msvec.hpp"
#include "hash/simpletab32.hpp"
#include "hash/tornado32.hpp"
#include "hash/rapidhash.h"
#include "core/r2.hpp"
struct View{ const uint8_t* p; uint32_t len; bool operator==(const View&o)const noexcept{ return len==o.len && std::memcmp(p,o.p,len)==0; } };
struct ViewHash{ size_t operator()(const View&v)const noexcept{ uint32_t h=2166136261u; for(uint32_t i=0;i<v.len;++i){ h^=v.p[i]; h*=16777619u;} return (size_t)h; } };
static double jtrue_halves(const datasets::R2& base){
  const auto& buf=base.buffer(); const auto& index=base.index(); size_t N=index.size(); size_t mid=N/2;
  std::unordered_set<View,ViewHash> A,B; A.reserve(mid); B.reserve(N-mid);
  for(size_t i=0;i<N;++i){ const auto [off,len]=index[i]; View v{buf.data()+off,(uint32_t)len}; if(i<mid) A.insert(v); else B.insert(v); }
  size_t inter=0; if(A.size()<B.size()) for(auto&v:A) inter+=B.count(v); else for(auto&v:B) inter+=A.count(v);
  size_t uni=A.size()+B.size()-inter; return uni? double(inter)/double(uni):1.0;
}
int main(int,char**){
  size_t K=200,R=50000; std::string outfile="oph_r2_relerr.csv";
  std::ofstream out(outfile,std::ios::binary); if(!out){ std::cerr<<"Cannot open "<<outfile<<"\n"; return 1;} out.setf(std::ios::fixed); out<<std::setprecision(8); out<<"function,rep,relerr\n";
  datasets::R2 base; const auto& buf=base.buffer(); const auto& index=base.index(); const size_t N=index.size(); const size_t mid=N/2;
  const double JT=jtrue_halves(base); const double denom=(JT>0.0?JT:1.0);
  enum{IDX_MSVEC,IDX_TABMS,IDX_T1,IDX_T2,IDX_T3,IDX_T4,IDX_RAPID,NUM}; const char* NAMES[NUM]={"MSVec","TabOnMSVec","TornadoOnMSVecD1","TornadoOnMSVecD2","TornadoOnMSVecD3","TornadoOnMSVecD4","RapidHash32"};
  using Coeffs=std::array<uint64_t,MSVEC_NUM_COEFFS>; struct P{ Coeffs c; uint64_t s; }; std::vector<P> V(R); for(size_t r=0;r<R;++r){ for(size_t i=0;i<MSVEC_NUM_COEFFS;++i)V[r].c[i]=rng::get_u64(); V[r].s=rng::get_u64(); }
  for(size_t r=0;r<R;++r){
    hashfn::MSVec ms; ms.set_params(V[r].c,true);
    hashfn::TabOnMSVec tab; tab.set_params(V[r].c,true);
    hashfn::TornadoOnMSVecD1 t1; t1.set_params(V[r].c,true);
    hashfn::TornadoOnMSVecD2 t2; t2.set_params(V[r].c,true);
    hashfn::TornadoOnMSVecD3 t3; t3.set_params(V[r].c,true);
    hashfn::TornadoOnMSVecD4 t4; t4.set_params(V[r].c,true);
    rapid::RapidHash32 rh; rh.set_params(V[r].s, rapid_secret[0], rapid_secret[1], rapid_secret[2]);
    sketch::OPH SA[NUM]={sketch::OPH((uint32_t)K),sketch::OPH((uint32_t)K),sketch::OPH((uint32_t)K),sketch::OPH((uint32_t)K),sketch::OPH((uint32_t)K),sketch::OPH((uint32_t)K),sketch::OPH((uint32_t)K)};
    sketch::OPH SB[NUM]={sketch::OPH((uint32_t)K),sketch::OPH((uint32_t)K),sketch::OPH((uint32_t)K),sketch::OPH((uint32_t)K),sketch::OPH((uint32_t)K),sketch::OPH((uint32_t)K),sketch::OPH((uint32_t)K)};
    for(size_t i=0;i<mid;++i){ const auto [off,len]=index[i]; const void* p=buf.data()+off;
      SA[IDX_MSVEC].push(ms.hash(p,len)); SA[IDX_TABMS].push(tab.hash(p,len)); SA[IDX_T1].push(t1.hash(p,len)); SA[IDX_T2].push(t2.hash(p,len)); SA[IDX_T3].push(t3.hash(p,len)); SA[IDX_T4].push(t4.hash(p,len)); SA[IDX_RAPID].push(rh.hash(p,len)); }
    for(size_t i=mid;i<N;++i){ const auto [off,len]=index[i]; const void* p=buf.data()+off;
      SB[IDX_MSVEC].push(ms.hash(p,len)); SB[IDX_TABMS].push(tab.hash(p,len)); SB[IDX_T1].push(t1.hash(p,len)); SB[IDX_T2].push(t2.hash(p,len)); SB[IDX_T3].push(t3.hash(p,len)); SB[IDX_T4].push(t4.hash(p,len)); SB[IDX_RAPID].push(rh.hash(p,len)); }
    for(int f=0;f<NUM;++f){ double Jest=sketch::jaccard(SA[f],SB[f]); out<<NAMES[f]<<","<<(r+1)<<","<<((Jest-JT)/denom)<<"\n"; }
  } std::cout<<"Done.\n"; return 0;
}