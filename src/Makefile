CC      =        gcc

CFLAGS  =        -std=gnu11 -g3 -O0 -fPIC -mtune=generic -mno-red-zone -pedantic -Werror-implicit-function-declaration -Wall
#CFLAGS	=        -std=gnu11 -DNDEBUG -O2 -fPIC -mtune=generic -mno-red-zone -pedantic -Werror-implicit-function-declaration -Wall

OBJECTS =        qprocessor.o QL.tab.o lex.QL_.o DELETE.tab.o lex.DELETE_.o PUT.tab.o lex.PUT_.o \
		 spatial_standard_queries.o skyline_queries.o rtree.o \
                 symbol_table.o priority_queue.o queue.o \
                 stack.o buffer.o swap.o common.o defs.o
                 #ntree.o

LIBS    =        -lpthread -lm 

install           : all
			sudo cp -vf "start#server" "create#rtree" "create#ntree" /usr/bin/
all               : start_server create_rtree #create_ntree
start_server      : start_server.c $(OBJECTS)
			$(CC) $(CFLAGS) -o "start#server" start_server.c $(OBJECTS) $(LIBS) 

qprocessor.o      : qprocessor.h QL.tab.o lex.QL_.o DELETE.tab.o lex.DELETE_.o PUT.tab.o lex.PUT_.o
			$(CC) $(CFLAGS) -c qprocessor.c -lfl 
QL.tab.o          : QL.tab.h lex.QL_.o
#QL.tab.c          : QL.y
#			bison --defines QL.y
lex.QL_.o          : 
#lex.QL_.c          : QL.l
#			flex QL.l 
DELETE.tab.o      : DELETE.tab.h lex.DELETE_.o
#DELETE.tab.c      : DELETE.y
#			bison --defines DELETE.y
lex.DELETE_.o     :
#lex.DELETE_.c     : DELETE.l
#			flex DELETE.l
PUT.tab.o         : PUT.tab.h lex.PUT_.o
#PUT.tab.c         : PUT.y
#			bison --defines PUT.y
lex.PUT_.o        :
#lex.PUT_.c        : PUT.l
#			flex PUT.l

#create_ntree       : ntree.o common.o symbol_table.o priority_queue.o queue.o stack.o buffer.o swap.o defs.o 
#			$(CC) $(CFLAGS) -o "create#ntree" create_ntree.c ntree.o common.o symbol_table.o priority_queue.o queue.o stack.o buffer.o swap.o defs.o $(LIBS) 
create_rtree       : rtree.o common.o symbol_table.o priority_queue.o queue.o stack.o buffer.o swap.o defs.o 
			$(CC) $(CFLAGS) -o "create#rtree" create_rtree.c rtree.o common.o symbol_table.o priority_queue.o queue.o stack.o buffer.o swap.o defs.o $(LIBS) 
spatial_standard_queries.o : spatial_standard_queries.h rtree.h priority_queue.h queue.h stack.h defs.h
skyline_queries.o : skyline_queries.h rtree.h priority_queue.h queue.h stack.h defs.h
network.o         : network.h symbol_table.h queue.h
ntree.o           : ntree.h common.h priority_queue.h queue.h stack.h defs.h
rtree.o           : rtree.h common.h priority_queue.h queue.h stack.h defs.h
common.o          : common.h priority_queue.h queue.h stack.h defs.h
symbol_table.o    : symbol_table.h stack.h queue.h defs.h
priority_queue.o  : priority_queue.h defs.h
queue.o           : queue.h defs.h
stack.o           : stack.h defs.h
buffer.o          : buffer.h defs.h
swap.o            : swap.h defs.h
defs.o            : defs.h


.PHONY  : all clean

clean   :
		-rm -f qprocessor main "start#server" "create#rtree" "create#ntree" $(OBJECTS) 

