# -*- coding:utf-8 -*-

import struct
import os
import os.path
import sys

PAGE_SIZE = 16*1024
FSP_HDR = PAGE_SIZE # locates in the first page of each file space 
IBUF_BITMAP = PAGE_SIZE # second page behinds FSP_HDR

def parse_file(file_name):
	
	if os.path.exists(file_name):
		_parse(file_name)
	else:
		print "please input the InnoDB file."
		sys.exit(1)

def _parse(file_name):

	bytes = _read_bytes(file_name, 0, FSP_HDR)
	if bytes:
		print "\t\t" + "-"*20 + "FSP_HDR_XDES (1 page = 16K)" + "-"*20
		_parse_FSP_HDR_XDES(bytes)
	else:
		print "parse FSP_HDR error"
		sys.exit(1)
	bytes = _read_bytes(file_name, FSP_HDR, FSP_HDR + PAGE_SIZE)
	if bytes:
		print "\t\t" + "-"*20 + "IBUF_BITMAP (1 page = 16K)" + "-"*20
		_parse_IBUF_BITMAP(bytes)
	else:
		print "parse IBUF_BITMAP error"
		sys.exit(1)		
	bytes = _read_bytes(file_name, FSP_HDR + IBUF_BITMAP, FSP_HDR + IBUF_BITMAP + PAGE_SIZE)
	if bytes:
		print "\t\t" + "-"*20 + "INODE (1 page = 16K)" + "-"*20
		_parse_INODE(bytes)
	else:
		print "parse INODE error"
		sys.exit(1)
	bytes = _read_bytes(file_name, FSP_HDR + IBUF_BITMAP + INODE, FSP_HDR + IBUF_BITMAP + INODE + PAGE_SIZE)
	if bytes:
		print "\t\t" + "-"*20 + "INSERT BUFFER HEADER (1 page = 16K)" + "-"*20
		_parse_IBH(bytes)
	else:
		print "parse IBUF_HEADER error"
		sys.exit(1)
	bytes = _read_bytes(file_name, FSP_HDR + IBUF_BITMAP + INODE + IBH, FSP_HDR + IBUF_BITMAP + INODE + IBH + PAGE_SIZE)
	if bytes:
		print "\t\t" + "-"*20 + "INDEX (1 page = 16K)" + "-"*20
		_parse_INDEX(bytes)
	else:
		print "parse INDEX error"
		sys.exit(1)

def _read_bytes(file_name, start, end):

	bytes = ""
	with open(file_name,'rb') as f:
		f.seek(start)
		bytes = f.read(end)
	return bytes

def _parse_FSP_HDR_XDES(bytes):
	FIL_Header,FSP_Header,XDES_Entrys,Empty_Space,FIL_Trailer= struct.unpack_from(">38s112s10240s5986s8s", bytes, 0)
	print "-"*10 + "FIL_HDR (38 bytes)" + "-"*10
	_parse_FIL_HDR(FIL_Header)
	print "-"*10 + "FSP_HDR_XDES (112 bytes)" + "-"*10
	_parse_FSP_HEADER(FSP_Header)
	print "-"*10 + "XDES_ENTRYS (10240 bytes)" + "-"*10
	_parse_XDES_ENTRYS(XDES_Entrys)
	print "-"*10 + "EMPTY_SPACE (5986 bytes)" + "-"*10
	_parse_Empty_Space(Empty_Space)
	print "-"*10 + "FIL_TRAILER (8 bytes)" + "-"*10
	_parse_FIL_Trailer(FIL_Trailer)

def _parse_IBUF_BITMAP(bytes):
	FIL_Header,Change_Buffer_Bitmap,Empty_Space,FIL_Trailer= struct.unpack_from(">38s8192s8146s8s", bytes)
	print "-"*10 + "FIL_HDR (38 bytes)" + "-"*10
	_parse_FIL_HDR(FIL_Header)
	print "-"*10 + "Change_Buffer_Bitmap (8192 bytes)" + "-"*10
	_parse_Change_Buffer_Bitmap(Change_Buffer_Bitmap)
	print "-"*10 + "Empty Space (8146 bytes)" + "-"*10
	_parse_Empty_Space(Empty_Space)
	print "-"*10 + "FIL_Trailer (8 bytes)" + "-"*10
	_parse_FIL_Trailer(FIL_Trailer)

def _parse_INODE(bytes):
	FIL_Header,Inode_Page_List,Inodes,Empty_Space,FIL_Trailer = struct.unpack_from(">38s12s16320s6s8s", bytes)
	print "-"*10 + "FIL_HDR (38 bytes)" + "-"*10
	_parse_FIL_HDR(FIL_Header)
	print "-"*10 + "Inode_Page_list (12 bytes)" + "-"*10
	_parse_Inode_Page_List(Inode_Page_List)
	print "-"*10 + "Inodes (total 16320 bytes)" + "-"*10
	_parse_Inodes(Inodes)
	print "-"*10 + "Empty_Space (6 bytes)" + "-"*10
	_parse_Empty_Space(Empty_Space)
	print "-"*10 + "FIL_Trailer (8 bytes)" + "-"*10
        _parse_FIL_Trailer(FIL_Trailer)

def _parse_IBH(bytes):
	pass

def _parse_INDEX(bytes):
	FIL_Header,Index_Header,Fseg_Header,System_Records,Var,FIL_Trailer = struct.unpack_from(">38s36s20s26s162568s", bytes)
        print "-"*10 + "FIL_HDR (38 bytes)" + "-"*10
        _parse_FIL_HDR(FIL_Header)
        print "-"*10 + "Inode_Page_list (12 bytes)" + "-"*10
        _parse_Index_Header(Index_Header)
        print "-"*10 + "Inodes (total 16320 bytes)" + "-"*10
        _parse_Fseg_Header(Fseg_Header)
        print "-"*10 + "Empty_Space (6 bytes)" + "-"*10
        _parse_System_Records(System_Records)
        print "-"*10 + "FIL_Trailer (8 bytes)" + "-"*10
	_parse_Var(Var)
        _parse_FIL_Trailer(FIL_Trailer)

def _parse_FIL_HDR(bytes):
	'''用来记录页的头信息，总共38 bytes'''	
	fil_page_space_or_chksum,fil_page_offset,fil_page_prev,fil_page_next,fil_page_lsn,\
	fil_page_type,fil_page_file_flush_lsn,fil_page_arch_log_no_or_space_id = struct.unpack(">Iiiiqhqi",bytes)
	print "page_checksum:%d\tpage_offset:%d" % (fil_page_space_or_chksum,fil_page_offset)
	print "prev_page_offset:%d\tnext_page_offset:%d" % (fil_page_prev,fil_page_next) 
	print "last_modify_LSN:%d\tpage_type:%d\tlast_flush_LSN:%d\tpage_space_id:%d" % \
		(fil_page_lsn,fil_page_type,fil_page_file_flush_lsn,fil_page_arch_log_no_or_space_id)

def _parse_FSP_HEADER(bytes):
	'''只在每个文件空间的第一个页中存在，其它情况都填0，总共112 bytes'''
	space_id,unused,h_pnum_in_file,h_pnum_init,flags,frag_page_used,list_base_node_for_free_list, \
	list_base_node_for_free_frag_list,list_base_node_for_full_frag_list,next_unused_segment_id, \
	list_base_node_for_full_inodes_list,list_base_node_for_free_inodes_list = struct.unpack(">iiiiii16s16s16sq16s16s",bytes)
	def parse_base_node(list_base_node):
		list_length,first_page_number,f_offset,last_page_number,l_offset = struct.unpack(">iihih",list_base_node)
		return list_length,first_page_number,f_offset,last_page_number,l_offset
	print "current_space_id:%d\tunused:%d\th_page_size:%d\th_page_limit:%d" % (space_id,unused,h_pnum_in_file,h_pnum_init)
	print "storage_type_flags:%d\tpages_usedin_freefrag_list:%d" % (flags,frag_page_used)
	print "list_base_node_for_free：", parse_base_node(list_base_node_for_free_list)
	print "list_base_node_for_free_frag：",parse_base_node(list_base_node_for_free_frag_list)
	print "list_base_node_for_full_frag：",parse_base_node(list_base_node_for_full_frag_list)
	print "next_unused_segment_id:%d" % next_unused_segment_id
	print "list_base_node_for_full_inodes：",parse_base_node(list_base_node_for_full_inodes_list)
	print "list_base_node_for_free_inodes：",parse_base_node(list_base_node_for_free_inodes_list)

def _parse_XDES_ENTRYS(bytes):
	'''每个XDES_ENTRY描述一个extent区域，一个extent区域由64个页组成。每个entry总共40 bytes，共有256个entry'''
	for i in range(256):
		file_segment_id,pre_page_num,pre_offset,next_page_num,next_offset,state,\
							page_state_bitmap = struct.unpack_from(">qihihi16s", bytes, i*40)
		print "XDES Entry %d：" % i, file_segment_id,pre_page_num,pre_offset,next_page_num,next_offset,state

def _parse_Empty_Space(bytes):
	pass

def _parse_FIL_Trailer(bytes):
	higher_32b_of_lsn,lower_32b_of_lsn = struct.unpack(">II",bytes)
	print "FIL_Trailer（8 bytes）：",higher_32b_of_lsn,lower_32b_of_lsn

def _parse_Change_Buffer_Bitmap(bytes):
	'''4 bits per page, total 8192 bytes, indicates 16384 pages. 2 entrys per byte'''
	pass

def _parse_Inode_Page_List(bytes):
	'''6 bytes for prev page, 6 bytes for next page'''
	prev_page_num, prev_page_offset, next_page_num, next_page_offset = struct.unpack(">ihih",bytes)
	print "prev_page_num:%d\tprev_page_offset:%d\t next_page_num:%d\tnext_page_offset:%d" % \
					(prev_page_num, prev_page_offset, next_page_num, next_page_offset)
def _parse_Inodes(bytes):
	'''85 entrys, 192 bytes per entry'''
	def parse_base_node(bytes):
		list_len,prev_num,prev_offset,next_num,next_offset = struct.unpack(">iihih",bytes)
		return list_len,prev_num,prev_offset,next_num,next_offset	
	def parse_frag_array(bytes):
		for j in range(32):
			indivisual_page_num, = struct.unpack_from(">i",bytes,j*4)
			print "array entry:%d\tpage_num:%d" % (j, indivisual_page_num)
	for i in range(85):
		fseg_id,used_pages,free_list,not_full_list,full_list,magic_num,fragment_array_entrys = \
					struct.unpack_from(">qi16s16s16si128s",bytes,i*192)
		print "inode_entry %d：" % i,
		print "fseg_id:%d\tused_pages_in_notfull_list:%d\t" % (fseg_id,used_pages)
		print "free_list：", parse_base_node(free_list)
		print "not_full_list：",parse_base_node(not_full_list)
		print "full_list：",parse_base_node(full_list)
		print "magic number: ",magic_num
		print "fragment_array" 
		parse_frag_array(fragment_array_entrys)

if __name__ == '__main__':

	parse_file("/data1/mysql3309/ibdata1")
