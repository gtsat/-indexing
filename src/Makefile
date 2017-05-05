CC      =        gcc

CFLAGS	=        -std=gnu11 -DNDEBUG -O2 -fPIC -mglibc -mtune=generic -mno-red-zone
#CFLAGS  =        -std=gnu11 -g3 -O0 -fPIC -mglibc -mtune=generic -mno-red-zone

OBJECTS =        spatial_standard_queries.o \
                 qprocessor.o QL.tab.o lex.yy.o \
                 skyline_queries.o rtree.o \
                 symbol_table.o priority_queue.o queue.o \
                 stack.o buffer.o swap.o defs.o

LIBS    =        -lpthread -lm 

install           : all
			sudo cp -vf "start#server" "create#rtree" /usr/bin/
all               : start_server create_rtree
start_server      : start_server.c $(OBJECTS)
			$(CC) $(CFLAGS) -o "start#server" start_server.c $(OBJECTS) $(LIBS) 
qprocessor.o      : qprocessor.h QL.tab.o lex.yy.o
			$(CC) $(CFLAGS) -c qprocessor.c -lfl 
QL.tab.o          : QL.tab.h
#QL.tab.c          : QL.y
#			bison --defines QL.y
lex.yy.o          : 
#lex.yy.c          : QL.l
#			flex QL.l 
create_rtree       : rtree.o symbol_table.o priority_queue.o queue.o stack.o buffer.o swap.o defs.o 
			$(CC) $(CFLAGS) -o "create#rtree" create_rtree.c rtree.o symbol_table.o priority_queue.o queue.o stack.o buffer.o swap.o defs.o $(LIBS) 
spatial_standard_queries.o : spatial_standard_queries.h rtree.h priority_queue.h queue.h stack.h defs.h
skyline_queries.o : skyline_queries.h rtree.h priority_queue.h queue.h stack.h defs.h
rtree.o           : rtree.h priority_queue.h queue.h stack.h defs.h
symbol_table.o    : symbol_table.h stack.h queue.h defs.h
priority_queue.o  : priority_queue.h defs.h
queue.o           : queue.h defs.h
stack.o           : stack.h defs.h
buffer.o          : buffer.h defs.h
swap.o            : swap.h defs.h
defs.o            : defs.h


.PHONY  : all clean

clean   :
		-rm -f qprocessor main "create#rtree" "start#server" $(OBJECTS) 
#lex.yy.c QL.tab.h QL.tab.c

