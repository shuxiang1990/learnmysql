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
		_parse_FSP_HDR_XDES(bytes)
	else:
		print "parse FSP_HDR error"
		sys.exit(1)
	bytes = _read_bytes(file_name, FSP_HDR, FSP_HDR + PAGE_SIZE)
	if bytes:
		_parse_IBUF_BITMAP(bytes)
	else:
		print "parse IBUF_BITMAP error"
		sys.exit(1)		
	bytes = _read_bytes(file_name, FSP_HDR + IBUF_BITMAP, FSP_HDR + IBUF_BITMAP + PAGE_SIZE)
	if bytes:
		_parse_INODE(bytes)
	else:
		print "parse INODE error"
		sys.exit(1)

def _read_bytes(file_name, start, end):

	bytes = ""
	with open(file_name,'rb') as f:
		f.seek(start)
		bytes = f.read(end)
	return bytes

def _parse_FSP_HDR_XDES(bytes):
	FIL_Header,FSP_Header,XDES_Entrys,Empty_Space,FIL_Trailer= struct.unpack_from(">38s112s10240s5986s8s", bytes, 0)
	_parse_FIL_HDR(FIL_Header)
	_parse_FSP_HEADER(FSP_Header)
	_parse_XDES_ENTRYS(XDES_Entrys)
	_parse_Empty_Space(Empty_Space)
	_parse_FIL_Trailer(FIL_Trailer)

def _parse_IBUF_BITMAP(bytes):
	FIL_Header,Change_Buffer_Bitmap,Empty_Space,FIL_Trailer= struct.unpack_from(">38s8192s8146s8s", bytes)
	_parse_FIL_HDR(FIL_Header)
	_parse_Change_Buffer_Bitmap(Change_Buffer_Bitmap)
	_parse_Empty_Space(Empty_Space)
	_parse_FIL_Trailer(FIL_Trailer)

def _parse_INODE(bytes):
	FIL_Header,Inode_Page_List,Inodes,Empty_Space,FIL_Trailer = struct.unpack_from(">38s12s16320s6s8s", bytes)
	_parse_FIL_HDR(FIL_Header)
	_parse_Inode_Page_List(Inode_Page_List)
	_parse_Inodes(Inodes)
	_parse_Empty_Space(Empty_Space)
        _parse_FIL_Trailer(FIL_Trailer)

def _parse_FIL_HDR(bytes):
	'''用来记录页的头信息，总共38 bytes'''	
	fil_page_space_or_chksum,fil_page_offset,fil_page_prev,fil_page_next,fil_page_lsn,\
	fil_page_type,fil_page_file_flush_lsn,fil_page_arch_log_no_or_space_id = struct.unpack(">Iiiiqhqi",bytes)
	print "FIL_HDR（38 bytes）：",fil_page_space_or_chksum,fil_page_offset,fil_page_prev,fil_page_next,\
		fil_page_lsn,fil_page_type,fil_page_file_flush_lsn,fil_page_arch_log_no_or_space_id

def _parse_FSP_HEADER(bytes):
	'''只在每个文件空间的第一个页中存在，其它情况都填0，总共112 bytes'''
	space_id,unused,h_pnum_in_file,h_pnum_init,flags,frag_page_used,free_extends,free_frag,full_frag,\
	next_unused_segment_id,seg_inodes_full,seg_inodes_free = struct.unpack(">iiiiii16s16s16sq16s16s",bytes)
	print space_id,unused,h_pnum_in_file,h_pnum_init,flags,frag_page_used,free_extends,free_frag,full_frag,\
        next_unused_segment_id,seg_inodes_full,seg_inodes_free

def _parse_XDES_ENTRYS(bytes):
	pass

def _parse_Empty_Space(bytes):
	pass

def _parse_FIL_Trailer(bytes):
	higher_32b_of_lsn,lower_32b_of_lsn = struct.unpack(">II",bytes)
	print "FIL_Trailer（8 bytes）：",higher_32b_of_lsn,lower_32b_of_lsn

def _parse_Change_Buffer_Bitmap(bytes):
	pass

def _parse_Inode_Page_List(Inode_Page_List):
	pass
def _parse_Inodes(Inodes):
	pass

if __name__ == '__main__':

	parse_file("/data1/mysql3309/ibdata1")
