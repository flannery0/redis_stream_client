#ifndef _REDIS_TOOL_H
#define _REDIS_TOOL_H

#include <hiredis/hiredis.h>
#include <string>
#include <iostream>
#include <sstream>
#include <string.h>
#include <vector>
#include <map>
#include <pthread.h>

class RedisTool
{
    public:
        RedisTool(std::string address, int  port, std::string password):address(address), port(port), connected(false), pRedisContext(NULL),password(password)
        {
        }
	
	    RedisTool(std::string address, int  port):address(address), port(port), 
                connected(false), pRedisContext(NULL)
        {
        }

        int Connect();
        int Close();

        //stream function
	    int XADD(const std::string channel, const std::map<std::string,std::string> field_string_map);
	    int XREAD(const std::string channel, const int timeout=0);
	    int Publish(const std::string channel,std::map<std::string, std::string> field_string_map);
        int XgroupCreate(std::string channel, std::string group);
	    int XgroupRead(std::string channel,std::string group, std::string consumer, std::string &delivered_id, std::map<std::string,std::string> &MsgMap, int number=1);//异步多消费者消费
	    int XACK(std::string channel,  std::string group, std::string delivered_id);
	    int NextMsgId(const std::string channel, std::string start, std::string end, int count);//return 待处理
	    int XDEL(std::string channel, std::string delivered_id);

        int CreateConsumerGroup(std::string channel,  std::string group, int reconn);
	    int ConsumeMsg(std::string channel, std::string group, std::string consumer, std::string &delivered_id, std::map<std::string,std::string> &MsgMap, int number, int reconn);
	    int DeleteMsg(std::string channel,std::string delivered_id, int reconn);
	    int ACK(std::string channel, std::string group, std::string delivered_id,int reconn);
        bool GetConnected()
        {
            return connected;
        }
        int SetConnected(bool connected)
        {
            connected = connected;
            return 0;
        }


    private:
        std::string address;
        int port;
        std::string password;
        bool connected;
        redisContext *pRedisContext;
        pthread_mutex_t redis_tool_mutex;
};
#endif