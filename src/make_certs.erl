%%
%% %CopyrightBegin%
%% 
%% Copyright Ericsson AB 2007-2012. All Rights Reserved.
%% 
%% The contents of this file are subject to the Erlang Public License,
%% Version 1.1, (the "License"); you may not use this file except in
%% compliance with the License. You should have received a copy of the
%% Erlang Public License along with this software. If not, it can be
%% retrieved online at http://www.erlang.org/.
%% 
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and limitations
%% under the License.
%% 
%% %CopyrightEnd%
%%

-module(make_certs).
-compile([export_all]).

-export([all/1, all/2, rootCA/2, intermediateCA/3, endusers/3, enduser/3, revoke/3, gencrl/2, verify/3]).

-record(dn, {commonName, 
	     organizationalUnitName = "Basho Engineering",
	     organizationName = "Basho",
	     localityName = "Cambridge, MA",
	     countryName = "US",
	     emailAddress = "riak@basho.com",
	     default_bits = "2048"}).

-define(OpenSSLCmd, "openssl").

all([DataDir, PrivDir]) ->
	all(DataDir, PrivDir).

all(DataDir, PrivDir) ->
    ok = filelib:ensure_dir(filename:join(PrivDir, "erlangCA")),
    create_rnd(DataDir, PrivDir),			% For all requests
    rootCA(PrivDir, "erlangCA"),
    intermediateCA(PrivDir, "otpCA", "erlangCA"),
    intermediateCA(PrivDir, "revokedCA", "erlangCA"),
    intermediateCA(PrivDir, "bashoCA", "otpCA"),
    endusers(PrivDir, "otpCA", ["client", "server", "revoked"]),
    endusers(PrivDir, "erlangCA", ["localhost"]),
    endusers(PrivDir, "revokedCA", ["blunderbuss"]),
    endusers(PrivDir, "bashoCA", ["scuttlebutt"]),
    %% Create keycert files 
    SDir = filename:join([PrivDir, "server"]),
    SC = filename:join([SDir, "cert.pem"]),
    SK = filename:join([SDir, "key.pem"]),
    SKC = filename:join([SDir, "keycert.pem"]),
    append_files([SK, SC], SKC),
    CDir = filename:join([PrivDir, "client"]),
    CC = filename:join([CDir, "cert.pem"]),
    CK = filename:join([CDir, "key.pem"]),
    CKC = filename:join([CDir, "keycert.pem"]),
    append_files([CK, CC], CKC),
    RDir = filename:join([PrivDir, "revoked"]),
    RC = filename:join([RDir, "cert.pem"]),
    RK = filename:join([RDir, "key.pem"]),
    RKC = filename:join([RDir, "keycert.pem"]),
    append_files([RK, RC], RKC),
    ok = verify(PrivDir, "erlangCA", "localhost"),
    ok = verify(PrivDir, "otpCA", "client"),
    ok = verify(PrivDir, "otpCA", "revoked"),
    revoke(PrivDir, "otpCA", "revoked"),
    revoke(PrivDir, "erlangCA", "revokedCA"),
    invalid = verify(PrivDir, "otpCA", "revoked"),
    invalid = verify(PrivDir, "otpCA", "revokedCA"),
    remove_rnd(PrivDir).

append_files(FileNames, ResultFileName) ->
    {ok, ResultFile} = file:open(ResultFileName, [write]),
    do_append_files(FileNames, ResultFile).

do_append_files([], RF) ->
    ok = file:close(RF);
do_append_files([F|Fs], RF) ->
    {ok, Data} = file:read_file(F),
    ok = file:write(RF, Data),
    do_append_files(Fs, RF).

rootCA(Root, Name) ->
    create_ca_dir(Root, Name, ca_cnf(Name)),
    DN = #dn{commonName = Name},
    create_self_signed_cert(Root, Name, req_cnf(DN)),
    file:copy(filename:join([Root, Name, "cert.pem"]), filename:join([Root, Name, "cacerts.pem"])),
    gencrl(Root, Name).

intermediateCA(Root, CA, ParentCA) ->
    create_ca_dir(Root, CA, ca_cnf(CA)),
    CARoot = filename:join([Root, CA]),
    DN = #dn{commonName = CA},
    CnfFile = filename:join([CARoot, "req.cnf"]),
    file:write_file(CnfFile, req_cnf(DN)),
    KeyFile = filename:join([CARoot, "private", "key.pem"]), 
    ReqFile =  filename:join([CARoot, "req.pem"]), 
    create_req(Root, CnfFile, KeyFile, ReqFile),
    CertFile = filename:join([CARoot, "cert.pem"]),
    sign_req(Root, ParentCA, "ca_cert", ReqFile, CertFile),
    CACertsFile = filename:join(CARoot, "cacerts.pem"),
    file:copy(filename:join([Root, ParentCA, "cacerts.pem"]), CACertsFile),
    %% append this CA's cert to the cacerts file
    {ok, Bin} = file:read_file(CertFile),
    {ok, FD} = file:open(CACertsFile, [append]),
    file:write(FD, ["\n", Bin]),
    file:close(FD),
    gencrl(Root, CA).

endusers(Root, CA, Users) ->
    [enduser(Root, CA, User) || User <- Users].

enduser(Root, CA, {User, DirName}) ->
    enduser(Root, CA, User, DirName);

enduser(Root, CA, User) ->
    enduser(Root, CA, User, User).

enduser(Root, CA, User, DirName) ->
    UsrRoot = filename:join([Root, DirName]),
    file:make_dir(UsrRoot),
    CnfFile = filename:join([UsrRoot, "req.cnf"]),
    DN = #dn{commonName = User},
    file:write_file(CnfFile, req_cnf(DN)),
    KeyFile = filename:join([UsrRoot, "key.pem"]), 
    ReqFile =  filename:join([UsrRoot, "req.pem"]), 
    create_req(Root, CnfFile, KeyFile, ReqFile),
    %create_req(Root, CnfFile, KeyFile, ReqFile),
    CertFileAllUsage =  filename:join([UsrRoot, "cert.pem"]),
    sign_req(Root, CA, "user_cert", ReqFile, CertFileAllUsage),
    CertFileDigitalSigOnly =  filename:join([UsrRoot, "digital_signature_only_cert.pem"]),
    sign_req(Root, CA, "user_cert_digital_signature_only", ReqFile, CertFileDigitalSigOnly),
    CACertsFile = filename:join(UsrRoot, "cacerts.pem"),
    file:copy(filename:join([Root, CA, "cacerts.pem"]), CACertsFile),
    ok.

revoke(Root, CA, User) ->
    UsrCert = filename:join([Root, User, "cert.pem"]),
    CACnfFile = filename:join([Root, CA, "ca.cnf"]),
    Cmd = [?OpenSSLCmd, " ca"
	   " -revoke ", UsrCert,
	   " -crl_reason keyCompromise",
	   " -config ", CACnfFile],
    Env = [{"ROOTDIR", filename:absname(Root)}], 
    cmd(Cmd, Env),
    gencrl(Root, CA).

gencrl(Root, CA) ->
    CACnfFile = filename:join([Root, CA, "ca.cnf"]),
    CACRLFile = filename:join([Root, CA, "crl.pem"]),
    Cmd = [?OpenSSLCmd, " ca"
	   " -gencrl ",
	   " -crlhours 24",
	   " -out ", CACRLFile,
	   " -config ", CACnfFile],
    Env = [{"ROOTDIR", filename:absname(Root)}], 
    cmd(Cmd, Env).

verify(Root, CA, User) ->
    CAFile = filename:join([Root, User, "cacerts.pem"]),
    CACRLFile = filename:join([Root, CA, "crl.pem"]),
    CertFile = filename:join([Root, User, "cert.pem"]),
    Cmd = [?OpenSSLCmd, " verify"
	   " -CAfile ", CAFile,
	   " -CRLfile ", CACRLFile, %% this is undocumented, but seems to work
	   " -crl_check ",
	   CertFile],
    Env = [{"ROOTDIR", filename:absname(Root)}],
    try cmd(Cmd, Env) catch
	   exit:{eval_cmd, _, _} ->
		invalid
    end.

create_self_signed_cert(Root, CAName, Cnf) ->
    CARoot = filename:join([Root, CAName]),
    CnfFile = filename:join([CARoot, "req.cnf"]),
    file:write_file(CnfFile, Cnf),
    KeyFile = filename:join([CARoot, "private", "key.pem"]), 
    CertFile = filename:join([CARoot, "cert.pem"]), 
    Cmd = [?OpenSSLCmd, " req"
	   " -new"
	   " -x509"
	   " -config ", CnfFile,
	   " -keyout ", KeyFile,
	   " -outform PEM",
	   " -out ", CertFile], 
    Env = [{"ROOTDIR", filename:absname(Root)}],  
    cmd(Cmd, Env).

create_self_signed_ecc_cert(Root, CAName, Cnf) ->
    CARoot = filename:join([Root, CAName]),
    CnfFile = filename:join([CARoot, "req.cnf"]),
    file:write_file(CnfFile, Cnf),
    KeyFile = filename:join([CARoot, "private", "key.pem"]), 
    CertFile = filename:join([CARoot, "cert.pem"]), 
    Cmd = [?OpenSSLCmd, " ecparam"
	   " -out ", KeyFile,
	   " -name secp521r1 ",
	   %" -name sect283k1 ",
	   " -genkey "],
    Env = [{"ROOTDIR", filename:absname(Root)}], 
    cmd(Cmd, Env),

    Cmd2 = [?OpenSSLCmd, " req"
	   " -new"
	   " -x509"
	   " -config ", CnfFile,
	   " -key ", KeyFile, 
		 " -outform PEM ",
	   " -out ", CertFile], 
    cmd(Cmd2, Env).

create_ca_dir(Root, CAName, Cnf) ->
    CARoot = filename:join([Root, CAName]),
    ok = filelib:ensure_dir(CARoot),
    file:make_dir(CARoot),
    create_dirs(CARoot, ["certs", "crl", "newcerts", "private"]),
    create_rnd(Root, filename:join([CAName, "private"])),
    create_files(CARoot, [{"serial", "01\n"},
			  {"crlnumber", "01"},
			  {"index.txt", ""},
			  {"ca.cnf", Cnf}]).

create_req(Root, CnfFile, KeyFile, ReqFile) ->
    Cmd = [?OpenSSLCmd, " req"
	   " -new"
	   " -config ", CnfFile,
	   " -outform PEM ",
	   " -keyout ", KeyFile, 
	   " -out ", ReqFile], 
    Env = [{"ROOTDIR", filename:absname(Root)}], 
    cmd(Cmd, Env).
    %fix_key_file(KeyFile).

create_ecc_req(Root, CnfFile, KeyFile, ReqFile) ->
    Cmd = [?OpenSSLCmd, " ecparam"
	   " -out ", KeyFile,
	   " -name secp521r1 ",
	   %" -name sect283k1 ",
	   " -genkey "],
    Env = [{"ROOTDIR", filename:absname(Root)}], 
    cmd(Cmd, Env),
    Cmd2 = [?OpenSSLCmd, " req"
	   " -new ",
	   " -key ", KeyFile,
	   " -outform PEM ",
	   " -out ", ReqFile,
	   " -config ", CnfFile],
    cmd(Cmd2, Env).
    %fix_key_file(KeyFile).


sign_req(Root, CA, CertType, ReqFile, CertFile) ->
    CACnfFile = filename:join([Root, CA, "ca.cnf"]),
    Cmd = [?OpenSSLCmd, " ca"
	   " -batch"
	   " -notext"
	   " -config ", CACnfFile, 
	   " -extensions ", CertType,
	   " -in ", ReqFile, 
	   " -out ", CertFile],
    Env = [{"ROOTDIR", filename:absname(Root)}], 
    cmd(Cmd, Env).
    
%%
%%  Misc
%%
    
create_dirs(Root, Dirs) ->
    lists:foreach(fun(Dir) ->
			  file:make_dir(filename:join([Root, Dir])) end,
		  Dirs).

create_files(Root, NameContents) ->
    lists:foreach(
      fun({Name, Contents}) ->
	      file:write_file(filename:join([Root, Name]), Contents) end,
      NameContents).

create_rnd(FromDir, ToDir) ->
     From = filename:join([FromDir, "RAND"]),
     To = filename:join([ToDir, "RAND"]),
     file:copy(From, To).

remove_rnd(Dir) ->
    File = filename:join([Dir, "RAND"]),
    file:delete(File).

cmd(Cmd, Env) ->
    FCmd = lists:flatten(Cmd),
    Port = open_port({spawn, FCmd}, [stream, eof, exit_status, stderr_to_stdout, 
				    {env, Env}]),
    eval_cmd(Port, FCmd).

eval_cmd(Port, Cmd) ->
    receive 
	{Port, {data, _}} ->
	    eval_cmd(Port, Cmd);
	{Port, eof} ->
	    ok
    end,
    receive
	{Port, {exit_status, Status}} when Status /= 0 ->
	    %% io:fwrite("exit status: ~w~n", [Status]),
	    exit({eval_cmd, Cmd, Status})
    after 0 ->
	    ok
    end.

%%
%% Contents of configuration files 
%%

req_cnf(DN) ->
    ["# Purpose: Configuration for requests (end users and CAs)."
     "\n"
     "ROOTDIR	        = $ENV::ROOTDIR\n"
     "\n"

     "[req]\n"
     "input_password	= secret\n"
     "output_password	= secret\n"
     "default_bits	= ", DN#dn.default_bits, "\n"
     "RANDFILE		= $ROOTDIR/RAND\n"
     "encrypt_key	= no\n"
     "default_md	= sha1\n"
     %% use string_mask to force the use of UTF8 for CN fields, as some OpenSSL installs
     %% end up creating 'teletexString' fields, which nobody supports any more (since 2003)
     "string_mask	= utf8only\n"
     "x509_extensions	= ca_ext\n"
     "prompt		= no\n"
     "distinguished_name= name\n"
     "\n"

     "[name]\n"
     "commonName		= ", DN#dn.commonName, "\n"
     "organizationalUnitName	= ", DN#dn.organizationalUnitName, "\n"
     "organizationName	        = ", DN#dn.organizationName, "\n" 
     "localityName		= ", DN#dn.localityName, "\n"
     "countryName		= ", DN#dn.countryName, "\n"
     "emailAddress		= ", DN#dn.emailAddress, "\n"
     "\n"

     "[ca_ext]\n"
     "basicConstraints 	= critical, CA:true\n"
     "keyUsage 		= cRLSign, keyCertSign\n"
     "subjectKeyIdentifier = hash\n"
     "subjectAltName	= email:copy\n"].

ca_cnf(CA) ->
    ["# Purpose: Configuration for CAs.\n"
     "\n"
     "ROOTDIR	        = $ENV::ROOTDIR\n"
     "default_ca	= ca\n"
     "\n"

     "[ca]\n"
     "dir		= $ROOTDIR/", CA, "\n"
     "certs		= $dir/certs\n"
     "crl_dir	        = $dir/crl\n"
     "database	        = $dir/index.txt\n"
     "new_certs_dir	= $dir/newcerts\n"
     "certificate	= $dir/cert.pem\n"
     "serial		= $dir/serial\n"
     "crl		= $dir/crl.pem\n"
     "crlnumber		= $dir/crlnumber\n"
     "private_key	= $dir/private/key.pem\n"
     "string_mask	= utf8only\n"
     "RANDFILE	        = $dir/private/RAND\n"
     "\n"
     "x509_extensions   = user_cert\n"
     "crl_extensions = crl_ext\n",
     "unique_subject  = no\n"
     "default_days	= 3600\n"
     "default_md	= sha256\n"
     "preserve	        = no\n"
     "policy		= policy_match\n"
     "\n"

     "[policy_match]\n"
     "commonName		= supplied\n"
     "organizationalUnitName	= optional\n"
     "organizationName	        = match\n"
     "countryName		= match\n"
     "localityName		= match\n"
     "emailAddress		= supplied\n"
     "\n"

     "[crl_ext]\n"
     "authorityKeyIdentifier=keyid:always,issuer:always\n"

     "[user_cert]\n"
     "basicConstraints	= CA:false\n"
     "keyUsage 		= nonRepudiation, digitalSignature, keyEncipherment\n"
     "subjectKeyIdentifier = hash\n"
     "authorityKeyIdentifier = keyid,issuer:always\n"
     "subjectAltName	= email:copy\n"
     "issuerAltName	= issuer:copy\n"
     "crlDistributionPoints=@crl_section\n"

     "[crl_section]\n"
     "URI.1=file://$ROOTDIR/",CA,"/crl.pem\n"
     "URI.2=http://localhost:8000/",CA,"/crl.pem\n"
     "\n"

     "[user_cert_digital_signature_only]\n"
     "basicConstraints	= CA:false\n"
     "keyUsage 		= digitalSignature\n"
     "subjectKeyIdentifier = hash\n"
     "authorityKeyIdentifier = keyid,issuer:always\n"
     "subjectAltName	= email:copy\n"
     "issuerAltName	= issuer:copy\n"
     "\n"

     "[ca_cert]\n"
     "basicConstraints 	= critical,CA:true\n"
     "keyUsage 		= cRLSign, keyCertSign\n"
     "subjectKeyIdentifier = hash\n"
     "authorityKeyIdentifier = keyid:always,issuer:always\n"
     "subjectAltName	= email:copy\n"
     "issuerAltName	= issuer:copy\n"
     "crlDistributionPoints=@crl_section\n"
    ].

