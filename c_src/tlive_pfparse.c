#include <stdlib.h>
#include <ctype.h>
#include <limits.h>
#include <string.h>
#include "erl_nif.h"

/* SYNTAX OF POLL FILES

poll_file = { header_line }, { poll_line } ;
header_line = "#", { all_characters }, "\n" ;
poll_line = timestamp, "|", datatype, "|", min, "|", max, "|", value, "|", site_code, "/",
			device_type, "/", device_id, "/", category, "/", instance, "_", metric, ".rrd" ;

timestamp = 10 * digit ;
datatype = upper, { upper };
min = "U" | integer ;
max = "U" | integer ;
site_code = 3 * upper ;
device_type = upper, { upper | lower | "_" };
device_id = integer ;
category = lower, { lower | "_" } ;
instance = lower, { lower | "_" } ;
metric = lower, { lower | "_" };
value = integer ;
integer = digit, { digit };  
*/

/* TODO:
 - Use array syntax for functions that take pointer arguments (e.g. char x[static 1] instead of char* x)
 - Use unsigned integer type for loop indices and handle wrap-around properly (look at the header than gives you the max constants
	and do e.g. if (i > MAX_UINT + 1) break;
 - 
*/

enum { 
	max_datatype_size = 10, 
	max_cat_size = 128, 
	max_inst_size = 128,
	ts_digits = 10, 
};

// Parser_state represents the state of the program when parsing a 
// poll-file line in the `parse_line` function.
enum parser_state {
	ps_ts,   /* parsing timestamp field */
	ps_dat,  /* parsing datatype field */
	ps_min,  /* parsing min field */
	ps_max,  /* parsing max field */
	ps_val,  /* parsing value field */
	ps_sc,   /* parsing site code field */
	ps_dvt,  /* parsing device type field */
	ps_did,  /* parsing device id field */
	ps_cat,  /* parsing category field */
	ps_inst, /* parsing instance field */
	ps_met,  /* parsing metric field */
    ps_eol,  /* finished parsing */
	ps_err,  /* parser error */
	ps_num,
};

typedef struct parser parser;
struct parser {
    enum parser_state st;
    char* p;   /* the current position of parser */
    char* p0;  /* points to the start byte of the current field */
    int st_c;    /* current parser state */
    int ts;      /* will hold timestamp field */
    int val;     /* will hold value field */ 
    int did;     /* bytes read in current state so far */
    char* cat;   /* will hold the category field */
    size_t cat_size;
    char* inst;  /* will hold the instance field */
    size_t inst_size;
    char* met;   /* will hold the metric field */
    size_t met_size;
    int err;     /* error code (0 is none) */
};

int const field_limits[ps_num] = {
	[ps_ts] = 10,
	[ps_dat] = 20,
	[ps_min] = INT_MAX, 
	[ps_max] = INT_MAX, 
	[ps_val] = INT_MAX, //TODO: should this be UINT_64MAX?
	[ps_sc] = 10, 
	[ps_dvt] = 255, 
	[ps_did] = INT_MAX, 
	[ps_cat] = INT_MAX,
	[ps_inst] = INT_MAX, 
	[ps_met] = INT_MAX,
	[ps_err] = 0,
};

int const field_delims[ps_num] = {
	[ps_ts] = '|',
	[ps_dat] = '|',
	[ps_min] = '|',
	[ps_max] = '|',
	[ps_val] = '|', 
	[ps_sc] = '/', 
	[ps_dvt] = '/', 
	[ps_did] = '/', 
	[ps_cat] = '/',
	[ps_inst] = '_',  //TODO: what's the trick here?
	[ps_met] = '.',
};

// Resource returned by `open` and passed to `decode_chunk`
typedef struct pfparseRes pfparseRes;
struct pfparseRes {
	FILE* fd; 		 /* file descriptor to poll-file */
	char* buf;       /* buffer containing left-over bytes from previous read */
	size_t buf_size; /* size of buffer */
    unsigned long int lo; /* number of left-over bytes */
};


int load(ErlNifEnv* env, void** priv_data, ERL_NIF_TERM load_info);
ERL_NIF_TERM mk_atom(ErlNifEnv* env, const char* atom);
ERL_NIF_TERM mk_error(ErlNifEnv* env, const char* mesg);
ERL_NIF_TERM mk_ok_result(ErlNifEnv* env, ERL_NIF_TERM result);
ERL_NIF_TERM mk_err_result(ErlNifEnv* env, ERL_NIF_TERM reason);
void pfparse_res_dtor(ErlNifEnv* env, void* res);
int skip_field(char** line, const int fs);
int verify_text_field(char** p, char** end, const int fs, const int max);
int parse_line(ErlNifEnv* env, const size_t n, char buffer[n], long unsigned int bytes_read[static 1], ERL_NIF_TERM parsed[static 1], int err[static 1]);
int check_delim(parser* psr);

ERL_NIF_TERM mk_atom(ErlNifEnv* env, const char* atom)
{
    ERL_NIF_TERM ret;

    if(!enif_make_existing_atom(env, atom, &ret, ERL_NIF_LATIN1))
    {
        return enif_make_atom(env, atom);
    }

    return ret;
}


ERL_NIF_TERM mk_error(ErlNifEnv* env, const char* mesg)
{
    return enif_make_tuple2(env, mk_atom(env, "error"), mk_atom(env, mesg));
}

ERL_NIF_TERM mk_ok_result(ErlNifEnv* env, ERL_NIF_TERM result)
{
	return enif_make_tuple2(env, mk_atom(env, "ok"), result);
}

ERL_NIF_TERM mk_err_result(ErlNifEnv* env, ERL_NIF_TERM reason)
{
	return enif_make_tuple2(env, mk_atom(env, "error"), reason);
}

void pfparse_res_dtor(ErlNifEnv* env, void* obj) 
{
	/* Clean up resource data.
	 * Note that the pointer to the resource struct 
	 * is actually deallocated by the Erlang garbage collector 
	 * since we called `enif_release_resource` in the `nif_open` function.
	 */
	pfparseRes* res = (pfparseRes*) obj;
	fclose(res->fd);
	enif_free(res->buf);
}

int load(ErlNifEnv* env, void** priv_data, ERL_NIF_TERM load_info)
{
	/*
	 * Here we create an (initially empty) resource type and store a pointer to it
	 * in `priv_data` so that we can access it when the NIFs are called.
	 * The resource type will be used to allocate storage for a `pfparseRes` so it 
	 * can be used between NIF calls and eventually deallocated by the garbage collector.
	 * The `pfparseRes` struct holds the file descriptor to the poll-file opened by `nif_open`
	 * and a buffer containing any left-over bytes from the last read.
	 *
	 * See https://erlang.org/doc/man/erl_nif.html#enif_resource_example for details
	 */

	int flags = ERL_NIF_RT_CREATE | ERL_NIF_RT_TAKEOVER;
	ErlNifResourceType* res_type = enif_open_resource_type(env, 0, "pfparse_res", pfparse_res_dtor, flags, 0);
	if (!res_type) return -1;
	*priv_data = (void*) res_type;
	return 0;
}

static ERL_NIF_TERM nif_open(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
	// Validate args
	if (argc != 2) return enif_make_badarg(env);
	
	// 1st arg: path to poll-file (list)
	unsigned int path_size; 
	if (!enif_get_list_length(env, argv[0], &path_size)) return enif_make_badarg(env);
	//char* path = malloc(sizeof(char[++path_size])); /* increment path_size for terminating '0' */
	char path[path_size + 1]; 
	if (!enif_get_string(env, argv[0], path, path_size + 1, ERL_NIF_LATIN1))
		return enif_make_badarg(env);

	// 2nd arg: buffer size (int)
	unsigned long int buf_size = 0; 
	if (!enif_get_ulong(env, argv[1], &buf_size) || buf_size <= 0) return enif_make_badarg(env);	

	// Retrieve resource type from private data
	ErlNifResourceType* res_type = (ErlNifResourceType*) enif_priv_data(env);
	// Allocate memory for resource struct using the resource type
	pfparseRes* res = (pfparseRes*) enif_alloc_resource(res_type, sizeof(pfparseRes));
	/* Allocate memory for file descriptor and buffer
	 * Note that we use `enif_alloc` here to allocate memory for the buffer instead of `malloc`. 
	 * This leaves it up the VM to allocate the memory, which might be faster. 
	 * Should do some benchmarking to find out.
	 */
	FILE* fd = fopen(path, "r");
	char* buf = (char*)enif_alloc(buf_size);
	if (!fd || !buf) return enif_make_badarg(env);
	buf[0] = 0; /* set first byte to null character */
	res->fd = fd;
	res->buf = buf;
	res->buf_size = buf_size; 
    res->lo = 0;

	ERL_NIF_TERM res_term = enif_make_resource(env, res);
	enif_release_resource(res);

	return mk_ok_result(env, res_term);
}

static ERL_NIF_TERM nif_decode_chunk(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
	if (argc != 1) return enif_make_badarg(env);
	ErlNifResourceType* res_type = (ErlNifResourceType*) enif_priv_data(env);
	pfparseRes* res;
	if (!enif_get_resource(env, argv[0], res_type, (void**)&res))
		return enif_make_badarg(env);

	unsigned int count = 0;
	//unsigned int total = 0;
	size_t term_arr_size = 100; /* TODO: you could base the initial guess of buffer size and the an "average" poll line size */
	ERL_NIF_TERM* term_arr = enif_alloc(sizeof(ERL_NIF_TERM) * term_arr_size);
    char* parse_buf = 0; /* points to start of bytes to parse */
    char* read_buf = 0; /* we will read next bytes from file into here */

    /*
     *   <---- res->lo -----> <---- res->buf_size ------>
     *  |____________________|__________________________|
     *  ^parse_buf           ^read_buf
     *  ^left overs go here  ^read bytes from file here
     */

    if (res->lo) {
        parse_buf = enif_alloc(sizeof(char) * (res->lo + res->buf_size));
        strcpy(parse_buf, res->buf); /* copy left over bytes to start of parse buffer */
    } else parse_buf = res->buf;

    read_buf = parse_buf + res->lo; /* read buffer points to just after left-over bytes in the parse buffer */

    unsigned long int bytes_read = 0;
    int err = 0;
	//while (fgets(read_buf, res->buf_size + 1 - total, res->fd)) {
    for (size_t total = 0; total < res->buf_size + res->lo; total += bytes_read) {
        if(!fgets(read_buf, res->buf_size + 1 - total + res->lo, res->fd)) break;
		if (count + 1 > term_arr_size) { 
			term_arr_size *= 2;
			if (!enif_realloc(term_arr, term_arr_size)) return enif_make_badarg(env);
		}
		if (parse_line(env, res->buf_size + res->lo, parse_buf, &bytes_read, &term_arr[count], &err)) {
            count += 1;
            res->lo = 0; /* reset left over count to 0 after first read */
            parse_buf = res->buf;
            read_buf = res->buf;
        } else break;
	}


    res->lo = bytes_read;
	ERL_NIF_TERM term_result = enif_make_list_from_array(env, term_arr, count);
    enif_free(term_arr);
    return term_result;
}

int parse_line(ErlNifEnv* env, const size_t n, char buffer[n], long unsigned int bytes_read[static 1], ERL_NIF_TERM parsed[static 1], int err[static 1])
{
    // TODO: function for initializing parser struct
    parser psr;
    psr.p = buffer;
    psr.p0 = buffer;
    psr.st = ps_ts;
    psr.ts = 0;
    psr.val = 0;
    psr.cat = 0;
    psr.cat_size = 0;
    psr.st_c = 0;
    psr.err = 0;

	for (; psr.p[0]; ++psr.p) {  /* assuming here that `buffer` is a string */
		if (psr.st_c > field_limits[psr.st] || psr.st == ps_err) {
			psr.st = ps_err;
			break;
		}
        //if (psr.st == ps_eol) break; /* reached end of line */
        if (check_delim(&psr)) continue; /* if current byte is a delimiter, advance the state and start from the top */
		switch (psr.st) {
			case ps_ts: 
				if (isdigit(*psr.p)) psr.st_c += 1;
				else psr.st = ps_err;
				break;
            case ps_val:
                if (isdigit(*psr.p)) psr.st_c += 1;
				else psr.st = ps_err;
                break;
            case ps_did:
                if (isdigit(*psr.p)) psr.st_c += 1;
				else psr.st = ps_err;
                break;
            case ps_cat:
                if (isprint(*psr.p)) psr.st_c += 1;
                else psr.st = ps_err;
                break;
            case ps_inst:
                if (isprint(*psr.p)) psr.st_c += 1;
                else psr.st = ps_err;
                break;
            case ps_dat:
            case ps_min:
            case ps_max:
            case ps_sc:
            case ps_dvt:
			default:
				//psr.st = ps_eol;
				break;
        }
    }

    *bytes_read = psr.p - buffer;
	int ret = 0;
	if (psr.st == ps_eol) {
	    ERL_NIF_TERM term_ts = enif_make_int(env, psr.ts);
	    ERL_NIF_TERM term_val = enif_make_int(env, psr.val);
	    ERL_NIF_TERM term_did = enif_make_int(env, psr.did);

	    ERL_NIF_TERM term_cat;
	    ERL_NIF_TERM term_inst;
        unsigned char* bin_cat = enif_make_new_binary(env, psr.cat_size, &term_cat);
        unsigned char* bin_inst = enif_make_new_binary(env, psr.inst_size, &term_inst);
        memcpy(bin_cat, psr.cat, psr.cat_size);
        memcpy(bin_inst, psr.inst, psr.inst_size);

        *parsed = enif_make_tuple5(env, term_ts, term_val, term_did, term_cat, term_inst);
        ret = 1;
    }

    err[0] = psr.st == ps_err? 1 : 0;
	return ret;
}

int check_delim(parser* psr)
{
    int ret = 0;
    if (*(psr->p) == field_delims[psr->st]) {
        switch (psr->st) {
            case ps_ts: psr->ts = strtod(psr->p0, 0); break;
            case ps_val: psr->val = strtod(psr->p0, 0); break;
            case ps_did: psr->did = strtod(psr->p0, 0); break;
            case ps_cat: 
                psr->cat = psr->p0; 
                psr->cat_size = psr->st_c;
                break;
            case ps_inst:
                psr->inst = psr->p0; 
                psr->inst_size = psr->st_c;
                break;
            case ps_dat:
            case ps_min:
            case ps_max:
            case ps_sc:
            case ps_dvt:
            default: break;
        }
        psr->p0 = psr->p + 1; /* set p0 to the byte directly after delimiter */
        psr->st += 1;
        psr->st_c = 0;
        ret = 1;
    }
    return ret;
}

//TODO: add max # of chars
int skip_field(char** line, const int fs) {
	while (*(*line)++ != fs)
		if (isspace(**line)) return 0;
	return 1;
}

int verify_text_field(char** p, char** end, const int fs, const int max) {
	*end = *p;
	while ((*++(*end)) != fs) {
		if (*end - *p > max || !isprint(**end) || isspace(**end))
			 return 0;
	}
	return 1;
}

static ErlNifFunc nif_funcs[] = {
    {"decode_chunk", 1, nif_decode_chunk},
    {"open", 2, nif_open}
};

ERL_NIF_INIT(tlive_pfparse, nif_funcs, load, (void*)0, (void*)0, (void*)0);
