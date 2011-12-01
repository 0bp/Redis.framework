//
//  Redis.h
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

#import <Foundation/Foundation.h>
#import "hiredis.h"

@interface Redis : NSObject {
  @private
  NSString * host;
  NSString * password;
  NSDate * lastCommandDate;
  redisContext * context;
  int port;
  int db;
}

- (id)initWithHost:(NSString *)host_ password:(NSString *)password port:(int)port_ db:(int)db_;
- (BOOL)selectDB:(int)db;
- (id)command:(NSString*)command;
- (id)commandArgv:(NSArray *)cargv;
- (id)getReply;
- (void)close;

@end


