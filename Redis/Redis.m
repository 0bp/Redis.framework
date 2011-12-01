//
//  Redis.m
//  Redis
//
//  Created by Boris Penck on 11-12-01.
//  Copyright (c) 2011 Boris Penck
// 
//  Permission is hereby granted, free of charge, to any person obtaining a copy
//  of this software and associated documentation files (the "Software"), to deal
//  in the Software without restriction, including without limitation the rights
//  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
//  copies of the Software, and to permit persons to whom the Software is
//  furnished to do so, subject to the following conditions:
//  
//  The above copyright notice and this permission notice shall be included in
//  all copies or substantial portions of the Software.
//  
//  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
//  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
//  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
//  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
//  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
//  OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
//  THE SOFTWARE.
//

#import "Redis.h"

@interface Redis (hidden)

@property (readonly) BOOL connected;

- (BOOL)connect;
- (BOOL)authenticate;
- (id)parseReply:(redisReply*)reply;
- (BOOL)timesOut;
- (const char**)vectorFromArray:(NSArray*)array;

@end

@implementation Redis

#pragma mark -
#pragma mark Public

- (id)initWithHost:(NSString *)host_ password:(NSString *)password_ port:(int)port_ db:(int)db_
{
    self = [super init];
    if (self) {
      lastCommandDate = [NSDate date];
      host = host_;
      password = password_;
      port = port_;
      db = db_;
      
      if(![self connect]) {
        self = nil;
      }
      
      if(password != nil && ![self authenticate]) {
        self = nil;
      }
    }
    
    return self;
}

#pragma mark -
#pragma mark Public

- (BOOL)connect
{
	context = redisConnect([host UTF8String], port);
  if (context->err != 0) {
    return NO;
  } else {
		return YES;
	}
}

- (void)close
{
	redisFree(context);
	context = NULL;
}

- (BOOL)selectDB:(int)db_ {
  [self command:[NSString stringWithFormat:@"SELECT %d", db]];
  return YES;
}

- (id)command:(NSString*)command
{
	if (!self.connected) return nil;
  
	redisReply *reply = redisCommand(context, [command UTF8String]);
	id retVal = [self parseReply:reply];
  freeReplyObject(reply);
	return retVal;
}

- (id)commandArgv:(NSArray *)cargv
{
	if (!self.connected) return nil;
  
	redisReply *reply = redisCommandArgv(context, (int)[cargv count], [self vectorFromArray:cargv], NULL);
	id retVal = [self parseReply:reply];
  freeReplyObject(reply);
	return retVal;
}


- (id)getReply
{
	[self timesOut];
	void * aux = NULL;
	NSMutableArray * replies = [NSMutableArray array];
  
	if (redisGetReply(context, &aux) == REDIS_ERR) { return nil; }
	if (aux == NULL) {
		int wdone = 0;
		while (!wdone) {
			if (redisBufferWrite(context,&wdone) == REDIS_ERR) {
				return nil;
			}
		}
    
		while(redisGetReply(context,&aux) == REDIS_OK) {
			redisReply * reply = (redisReply*)aux;
			[replies addObject:[self parseReply:reply]];
			freeReplyObject(reply);
		}
	} else {
		redisReply * reply = (redisReply*)aux;
		[replies addObject:[self parseReply:reply]];
		freeReplyObject(reply);
	}
  
	if ([replies count] > 1) {
		return [NSArray arrayWithArray:replies];
	} else if ([replies count] == 1) {
		return [replies objectAtIndex:0];
	} else {
		return nil;
	}
}

#pragma mark -
#pragma mark Private

- (BOOL)authenticate
{
  NSString * authentication = [self command:[NSString stringWithFormat:@"AUTH %@", password]];
  return [authentication isEqualToString:@"OK"];
}

- (BOOL)connected
{
  return context != NULL || !(context->flags & REDIS_CONNECTED) || context->err != 0;
}

- (void)dealloc {
  redisFree(context);
  context = NULL;
}

- (BOOL)timesOut
{
	NSDate * now = [NSDate date];
	NSTimeInterval elapsed = [now timeIntervalSinceDate:lastCommandDate];
	lastCommandDate = now;
  
	if (elapsed > (NSTimeInterval)300.0) {
		return YES;
	}
	return NO;
}


- (NSArray*)arrayFromVector:(redisReply **)vector ofSize:(long)size
{
	NSMutableArray * buildArray = [NSMutableArray arrayWithCapacity:size];
  
	for (int i = 0; i < size; i++) {
		if (vector[i] != NULL) {
			[buildArray addObject:[self parseReply:vector[i]]];
		} else {
			[buildArray addObject:nil];
		}
    
	}
	return [NSArray arrayWithArray:buildArray];
}

- (const char**)vectorFromArray:(NSArray*)array
{
  char * vector;
  vector = (char *)malloc([array count] + 1); 
  
  for (int i = 0; i < [array count]; i++) { 
    if ([[array objectAtIndex:i] isKindOfClass:[NSString class]]) {
      vector[i] = [[array objectAtIndex:i] cStringUsingEncoding:NSUTF8StringEncoding][0]; 
    }
  } 
  vector[[array count]] = 0; 
  
	return (const char **)vector;
}

- (id)parseReply:(redisReply*)reply
{
  switch (reply->type) {
    case REDIS_REPLY_ERROR:
    case REDIS_REPLY_STATUS:
    case REDIS_REPLY_STRING:
      return [NSString stringWithUTF8String:reply->str];
      break;
    case REDIS_REPLY_INTEGER:
      return [NSNumber numberWithLongLong:reply->integer];
      break;
    case REDIS_REPLY_ARRAY:
      return [self arrayFromVector:reply->element ofSize:reply->elements];
      break;
    case REDIS_REPLY_NIL:
    default:
      return nil;
      break;

  }
}  




@end
