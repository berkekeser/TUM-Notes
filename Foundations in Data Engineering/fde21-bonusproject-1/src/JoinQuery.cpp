#include "JoinQuery.hpp"
#include <charconv>
#include <fstream>
#include <unordered_map>
#include <fcntl.h>
#include <sys/mman.h>
#include <unistd.h>
#include <iostream>
#include <thread>
#include <assert.h>

using namespace std;

unordered_map<int, string> segmentMap;
unordered_map<int, int> orderMap;

unordered_map<string, uint64_t> sumOfLength;
unordered_map<string, uint64_t> segmentCustomerCountByType;

void readLineitemFile(string filePath)
{
   int handle = open(filePath.c_str(), O_RDONLY);
   lseek(handle, 0, SEEK_END);
   auto length = lseek(handle,0,SEEK_CUR);
   void* data = mmap(nullptr, length, PROT_READ, MAP_SHARED, handle, 0);
   auto begin = static_cast<const char*>(data), end = begin+length;

   int orderKeyInt, customerKey;
   unsigned quantity;
   string segment;

   for(auto iter=begin;iter<end;) {
      int flag = 1;
      string orderKey;
      const char* last = nullptr;
      unsigned col=0;

      for(;iter<end;++iter) {
         if ((*iter) != '|' && flag ==1) {
            orderKey += *iter;
         }
         if((*iter) == '|') {
            flag=0;
            ++col;
            if(col==4) {
               last=iter+1;
               from_chars(last,iter,quantity);
               while((iter<end)&&((*iter)!='\n'))
                  ++iter;
               if(iter<end)
                  ++iter;
               break;
            }
         }
      }
      orderKeyInt = stoi(orderKey);
      customerKey = orderMap[orderKeyInt];
      segment = segmentMap[customerKey];
      segmentCustomerCountByType[segment]+= 1;
      sumOfLength[segment] += quantity;
   }
   munmap(data, length);
   close(handle);
}

void readOrderFile(string file) {
   ifstream file1;
   file1.open(file);
   string line;
   while (getline(file1, line)) {
      string orderKey = "";
      string customerKey = "";
      int orderKeyInt, customerKeyInt;
      int flag=1;
      unsigned col = 0;
      for(char&c : line) {
         if(col==2) break;
         if (c != '|' && flag == 1) {
            orderKey += c;
         }
         if (c == '|') {
            ++col;
            flag = 0;
         }
         if (col == 1) {
            if (c == '|') continue;
            customerKey += c;
         }
      }

      orderKeyInt = stoi(orderKey);
      customerKeyInt = stoi(customerKey);
      orderMap[orderKeyInt] = customerKeyInt;
   }
   file1.close();
}

void readCustomerFile(string file)
{
   ifstream file1;
   file1.open(file);
   string line;
   while (getline(file1, line)) {
      unsigned col = 0;
      string segment;
      string customerKey;
      int customerKeyInt;
      int flag = 1;
      for (char& c : line) {
         if (col == 7) break;
         if (c != '|' && flag == 1) {
            customerKey += c;
            customerKeyInt = stoi(customerKey);
         }
         if (c == '|') {
            ++col;
            flag = 0;
         }
         if (col == 6) {
            if (c == '|') continue;
            segment += c;
         }
      }
      segmentMap[customerKeyInt] = segment;
   }
   file1.close();
}


//---------------------------------------------------------------------------
JoinQuery::JoinQuery(std::string lineitem, std::string order, std::string customer)
{
   segmentCustomerCountByType.clear();
   sumOfLength.clear();

   readCustomerFile(customer);
   readOrderFile(order);
   readLineitemFile(lineitem);

}
//---------------------------------------------------------------------------
size_t JoinQuery::avg(std::string segmentParam)
{
   return sumOfLength[segmentParam] * 100 / segmentCustomerCountByType[segmentParam];
}
//---------------------------------------------------------------------------
size_t JoinQuery::lineCount(std::string rel)
{
   std::ifstream relation(rel);
   assert(relation);  // make sure the provided string references a file
   size_t n = 0;
   for (std::string line; std::getline(relation, line);) n++;
   return n;
}
//---------------------------------------------------------------------------

