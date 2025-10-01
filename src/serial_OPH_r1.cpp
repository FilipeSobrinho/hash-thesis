// serial_OPH_r1.cpp
#include <algorithm>
#include <array>
#include <cstdint>
#include <cstring>
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
#include "core/r1.hpp"
struct Key20{ std::array<uint8_t,20>b{}; bool operator==(const Key20&o)const noexcept{ return std::memcmp(b.data(),o.b.data(),20)==0; } };
struct Key20Hash{ size_t operator()(const Key20&k)const noexcept{ uint32_t h=2166136261u; for(auto x:k.b){ h^=x; h*=16777619u;} return (size_t)h; } };
static double jtrue(const std::vector<Key20>&A,const std::vector<Key20>&B){
  std::unordered_set<Key20,Key20Hash> As; As.reserve(A.size()); std::unordered_set<Key20,Key20Hash> Bs; Bs.reserve(B.size());
  for(auto&k:A) As.insert(k); for(auto&k:B) Bs.insert(k); size_t inter=0;
  if(As.size()<Bs.size()) for(auto&k:As) inter+=Bs.count(k); else for(auto&k:B) inter+=As.count(k);
  size_t uni=As.size()+Bs.size()-inter; return uni? double(inter)/double(uni):1.0;
}
int main(int,char**){
  size_t K=200,R=50000; std::string outfile="oph_r1_relerr.csv";
  std::ofstream out(outfile,std::ios::binary); if(!out){ std::cerr<<"Cannot open "<<outfile<<"\n"; return 1;} out.setf(std::ios::fixed); out<<std::setprecision(8); out<<"function,rep,relerr\n";
  datasets::R1 base; const auto& raw=base.buffer(); const size_t N=base.size();
  auto splitbit=[](uint64_t i){ uint64_t x=i+0x9E3779B97F4A7C15ull; x=(x^(x>>30))*0xBF58476D1CE4E5B9ull; x=(x^(x>>27))*0x94D049BB133111EBull; return (x^(x>>31))&1ull; };
  std::vector<Key20>A,B; A.reserve(N/2+1024); B.reserve(N/2+1024);
  for(size_t i=0;i<N;++i){ Key20 k; std::memcpy(k.b.data(), raw.data()+i*20,20); if(splitbit(i)==0) A.push_back(k); else B.push_back(k); }
  const double JT=jtrue(A,B); const double denom=(JT>0.0?JT:1.0);
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
    for(auto&k:A){ const void*p=k.b.data(); SA[IDX_MSVEC].push(ms.hash(p,20)); SA[IDX_TABMS].push(tab.hash(p,20)); SA[IDX_T1].push(t1.hash(p,20)); SA[IDX_T2].push(t2.hash(p,20)); SA[IDX_T3].push(t3.hash(p,20)); SA[IDX_T4].push(t4.hash(p,20)); SA[IDX_RAPID].push(rh.hash(p,20)); }
    for(auto&k:B){ const void*p=k.b.data(); SB[IDX_MSVEC].push(ms.hash(p,20)); SB[IDX_TABMS].push(tab.hash(p,20)); SB[IDX_T1].push(t1.hash(p,20)); SB[IDX_T2].push(t2.hash(p,20)); SB[IDX_T3].push(t3.hash(p,20)); SB[IDX_T4].push(t4.hash(p,20)); SB[IDX_RAPID].push(rh.hash(p,20)); }
    for(int f=0;f<NUM;++f){ double Jest=sketch::jaccard(SA[f],SB[f]); out<<NAMES[f]<<","<<(r+1)<<","<<((Jest-JT)/denom)<<"\n"; }
  } std::cout<<"Done.\n"; return 0;
}