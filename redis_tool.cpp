#include "redis_tool.hpp"

int RedisTool::Connect()
{
	if (connected)
		return 1;
	pthread_mutex_lock(&redis_tool_mutex);//lock
	if (pRedisContext != NULL)  
    {  
        redisFree(pRedisContext);  
    }
    struct timeval timeout = {2, 0}; 
    pRedisContext = (redisContext*)redisConnectWithTimeout(address.c_str(), port, timeout);
    if ( (NULL == pRedisContext) || (pRedisContext->err)  )
    {
        redisFree(pRedisContext);
        pRedisContext = NULL;
		pthread_mutex_unlock(&redis_tool_mutex);//unlock	
        return -1;
    }

    struct timeval cmd_timeout = {30, 0}; 
    if (pRedisContext) {
    	redisSetTimeout(pRedisContext, cmd_timeout);
    	fprintf(stderr, "RedisAdapter::Set cmd timeout 30s!\n");
    }

    redisReply *pRedisReply = (redisReply*)redisCommand(pRedisContext, "AUTH %s", password.c_str() ); 
    if (pRedisReply == NULL) {
   		pthread_mutex_unlock(&redis_tool_mutex);//unlock	
        return -1;
    }

	int ret = -1;
    if (pRedisReply->type == REDIS_REPLY_ERROR  && strcmp(pRedisReply->str, "ERR Client sent AUTH, but no password is set") == 0)
    {
        ret = 0;
        connected = true;
    }
    else if (pRedisReply->type != REDIS_REPLY_STATUS)
    {
        ret = -1;
    }
    else if (strcmp(pRedisReply->str, "OK") == 0)
    {
        ret = 0;
        connected = true;
    }
    else
    {
        ret = -1;
    }
    freeReplyObject(pRedisReply);
	pthread_mutex_unlock(&redis_tool_mutex);//unlock	
    return ret;
}

int RedisTool::Close()
{
	pthread_mutex_lock(&redis_tool_mutex);//lock
    if (pRedisContext != NULL)
    {
        redisFree(pRedisContext);
        pRedisContext = NULL;
    }
    connected = false;
	pthread_mutex_unlock(&redis_tool_mutex);//unlock	
    return 0;
}

//put a meassage map into specific channel
int RedisTool::Publish(const std::string channel,std::map<std::string, std::string> field_string_map)
{
	if (!pRedisContext) return -1;
	std::string field_string;
	std::string command = "XADD";
	std::string format = "*";

	int command_length = field_string_map.size();
	const char **commandList = new const char*[3+2*command_length];
	commandList[0] = command.c_str();
	commandList[1] = channel.c_str();
	commandList[2] = format.c_str();

	size_t *lengthList = new size_t[3+2*command_length];
	lengthList[0] = command.length();
	lengthList[1] = channel.length();
	lengthList[2] = format.length();

	int index = 3;
	for(std::map<std::string,std::string>::const_iterator iter = field_string_map.begin(); iter != field_string_map.end(); ++iter)
	{
		commandList[index] = iter->first.c_str();
		lengthList[index] = iter->first.length();
		index++;
		commandList[index] = iter->second.c_str();
		lengthList[index] = iter->second.length();
		index++;
	}
	redisReply *pRedisReply = (redisReply*)redisCommandArgv(pRedisContext, index, (const char**)commandList, (const size_t*)lengthList); 
	fprintf(stderr, "RedisTool:: publish:%s: type:%d, str:%s\n", channel.c_str(), pRedisReply->type, pRedisReply->str);
    if (pRedisReply == NULL)
        return -1;
	if (pRedisReply->type == REDIS_REPLY_NIL)
    {
        freeReplyObject(pRedisReply);
        return -1;
    }
    else
    {
        freeReplyObject(pRedisReply);
        return 0;
    }	
}

//read channel messages
int RedisTool::XREAD(const std::string channel, int timeout)
{
	if (!pRedisContext) return -1;
	redisReply *pRedisReply = (redisReply*)redisCommand(pRedisContext, "XREAD BLOCK %d STREAMS %s $",timeout,channel.c_str()); 
    if (pRedisReply == NULL)
        return -1;
	if (pRedisReply->type != REDIS_REPLY_INTEGER)
    {
        freeReplyObject(pRedisReply);
        return -1;
    }
    else if (pRedisReply->integer > 0)
    {
        freeReplyObject(pRedisReply);
        return 1;
    }
    else
    {
        freeReplyObject(pRedisReply);
        return 0;
    }
}

//create a group able to consume messages of a channel
int RedisTool::XgroupCreate(std::string channel, std::string group)
{
	int ret = -1;
	if (!pRedisContext) return -1;
	redisReply *pRedisReply = (redisReply*)redisCommand(pRedisContext, "XGROUP CREATE %s %s 0",channel.c_str(),group.c_str()); 
    if (pRedisReply == NULL)
        return -1;
	else if (strcmp(pRedisReply->str, "OK") != 0)
	{
		fprintf(stderr, "RedisAdapter::Create Group %s of %s ERROR!\n",group.c_str(),channel.c_str());
		freeReplyObject(pRedisReply);
		return -1;
	}
	else
	{
		freeReplyObject(pRedisReply);
		ret = 1;
	}
    return ret;
}

//using consuming group read messages of a channel and put key&value into MsgMap
int RedisTool::XgroupRead(std::string channel, std::string group, std::string consumer, std::string &delivered_id, std::map<std::string,std::string> &MsgMap, int number)
{
	int ret = -1;
	if (!pRedisContext)
		return -1;
	redisReply *pRedisReply = (redisReply*)redisCommand(pRedisContext, "XREADGROUP GROUP %s %s count %d STREAMS %s >",group.c_str(),consumer.c_str(),number,channel.c_str()); 
	fprintf(stderr, "RedisAdapter::Init XREADGROUP GROUP %s %s count %d STREAMS %s %s!\n",group.c_str(),consumer.c_str(),number,channel.c_str(), delivered_id.c_str());
    if (pRedisReply == NULL)
	{
		fprintf(stderr, "RedisAdapter::Init ERROR:%s!\n",pRedisContext->errstr);
		return -1;
	}
	if (pRedisReply->type == REDIS_REPLY_NIL)
	{
		fprintf(stderr, "RedisAdapter::Group %s get messages of %s null,keep waiting!\n",group.c_str(),channel.c_str());
		freeReplyObject(pRedisReply);
        return 0;
	}
	else if (pRedisReply->type != REDIS_REPLY_ARRAY)
    {
		fprintf(stderr, "RedisAdapter::Group %s get messages of %s ERROR,type:%d,reply :%s!\n",group.c_str(),channel.c_str(),pRedisReply->type,pRedisReply->str);
        freeReplyObject(pRedisReply);
        return -1;
    }
	else
	{
		redisReply* msg_struct_0 = pRedisReply->element[0]->element[1];
		fprintf(stderr, "RedisAdapter::Group %s get messages of %s,type:%d.\n",group.c_str(),channel.c_str(),pRedisReply->type);
		std::string deliverkey = "delivered_id";
		delivered_id = msg_struct_0->element[0]->element[0]->str;
		MsgMap[deliverkey] = delivered_id;
		redisReply* msg_struct_1 = msg_struct_0->element[0]->element[1];
		for (int i = 0; i < msg_struct_1->elements; i += 2)
        {
			std::string key = msg_struct_1->element[i]->str;
			std::string value =  msg_struct_1->element[i+1]->str;
			MsgMap[key] = value;
			fprintf(stderr, "RedisAdapter::Group %s get messages of %s,key:%s,value :%s!\n",group.c_str(),channel.c_str(),key.c_str(),value.c_str());
		}
		ret = 1;
	}
    freeReplyObject(pRedisReply);
    return ret;
}

int RedisTool::XACK(std::string channel, std::string group, std::string delivered_id)
{
	int ret = -1;
	if (!pRedisContext) return -1;
	redisReply *pRedisReply = (redisReply*)redisCommand(pRedisContext, "XACK %s %s %s",channel.c_str(),group.c_str(),delivered_id.c_str()); 
    if (pRedisReply == NULL)
        return -1;
	if (pRedisReply->type == REDIS_REPLY_INTEGER)
		ret = pRedisReply->integer;
	else if (pRedisReply->type == REDIS_REPLY_ERROR  && strcmp(pRedisReply->str, "ERR Unknown or disabled command 'XACK'") == 0)
		fprintf(stderr, "RedisAdapter::Group %s ack message %s of %s error, because this message has already been acked!\n",group.c_str(),delivered_id.c_str(),channel.c_str());
	else
		fprintf(stderr, "RedisAdapter::Group %s ack message %s of %s error, type : %d, str:%s!\n",group.c_str(),delivered_id.c_str(),channel.c_str(),pRedisReply->type,pRedisReply->str);
	freeReplyObject(pRedisReply);
    return ret;
}

int RedisTool::NextMsgId(const std::string channel, std::string start, std::string end, int count=1)
{
	int ret = -1;
	if (!pRedisContext) return -1;
	redisReply *pRedisReply = (redisReply*)redisCommand(pRedisContext, "XRANGE %s %s %s COUNT %d",channel.c_str(),start.c_str(),end.c_str(),count); 
    if (pRedisReply == NULL)
        return -1;
	if (pRedisReply->type == REDIS_REPLY_NIL)
	{
		ret = 0;
	}
	else if(pRedisReply->type == REDIS_REPLY_ARRAY)
	{
		ret = atoi(pRedisReply->element[0]->str);
	}
	freeReplyObject(pRedisReply);
    return ret;
}

int RedisTool::XDEL(std::string channel, std::string delivered_id)
{
	int ret = -1;
	if (!pRedisContext) return -1;
	redisReply *pRedisReply = (redisReply*)redisCommand(pRedisContext, "XDEL %s %s",channel.c_str(),delivered_id.c_str()); 
    if (pRedisReply == NULL)
        return -1;
	else if (pRedisReply->type == REDIS_REPLY_INTEGER)
		ret = pRedisReply->integer;
	return ret;
}

int RedisTool::ConsumeMsg(std::string channel, std::string group, std::string consumer, std::string &delivered_id, std::map<std::string,std::string> &MsgMap, int number, int reconn)
{
	int ret = -1;
	int retry = 0;
	while (ret < 0) {
		if (!GetConnected())
		{
			Connect();
		}
		ret = XgroupRead(channel,group,consumer,delivered_id, MsgMap, number);
		if (ret < 0)
		{
			SetConnected(false);
			Close();
		}
		retry++;
		if (retry > reconn)
			break;
	}
	return ret;
}

int RedisTool::DeleteMsg(std::string channel, std::string delivered_id,int reconn)
{
	int ret = -1;
	int retry = 0;
	while (ret < 0) {
		if (!GetConnected())
		{
			Connect();
		}
		ret = XDEL(channel,delivered_id);
		if (ret < 0)
		{
			SetConnected(false);
			Close();
		}
		retry++;
		if (retry > reconn)
			break;
	}
	return ret;
}

int RedisTool::ACK(std::string channel, std::string group, std::string delivered_id,int reconn)
{
	int ret = -1;
	int retry = 0;
	while (ret < 0) {
		if (!GetConnected())
		{
			Connect();
		}
		ret = XACK(channel,group,delivered_id);
		if (ret < 0)
		{
			SetConnected(false);
			Close();
		}
		retry++;
		if (retry > reconn)
			break;
	}
	return ret;
}

