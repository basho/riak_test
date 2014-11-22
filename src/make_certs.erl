%% -------------------------------------------------------------------
%%
%% Copyright (c) 2014 Basho Technologies, Inc.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------

-module(make_certs).

%%  Macros below try to avoid needing this
%-compile([export_all]).

%%  This may have value at some point, but currently unused
%-define(INCLUDE_ECC_OPS, true).

%%  These are all just apparent cruftiness
%-define(INCLUDE_ALL, true).
%-define(INCLUDE_COLLECT, true).
%-define(INCLUDE_REMOVE_RND, true).

-export([
    create_priv_certs/0,
    test_cert_dir/0,
    priv_cert_dir/0,
    rootCA/2,
    intermediateCA/3,
    endusers/3,
    enduser/3, enduser/4,
    revoke/3,
    gencrl/2,
    verify/3
]).

-ifdef(INCLUDE_COLLECT).
-export([ collect_certs/3 ]).
-endif. %%  INCLUDE_COLLECT
-ifdef(INCLUDE_ALL).
-export([ all/1, all/2 ]).
-endif. %%  INCLUDE_ALL

-export_type([
    certtype/0,
    certsign/0,
    certpath/0,
    certname/0,
    certinfo/0
]).

-define(DEFAULT_ORG,        "Basho Engineering").
-define(DEFAULT_OU,         "Basho").
-define(DEFAULT_LOC,        "Cambridge, MA").
-define(DEFAULT_COUNTRY,    "US").
-define(DEFAULT_EMAIL,      "riak@basho.com").
-define(DEFAULT_BITS,       2048).

-type certname() :: string().   %%  A certificate subject CN (Common Name)
-type certpath() :: file:filename().  %%  Unambiguous path to a single file.

%%  @type   certtype()  The type of a referenced certificate.
%%  A 'ca' certificate represents a signing authority, either root or
%%  intermediate.  A 'user' certificate identifies a named entity.
%%  @see    certsign()
-type certtype() :: ca      %%  CA cert for signing
                 |  user.   %%  Signed cert for authentication

%%  @type   certsign()  Who this cert is signed by.
%%  A self-signed certificate (in our model) is always a Root CA.
%%  A certificate signed by a CA may be either an Intermediate CA (if the
%%  associated certtype() is 'ca') or an End-User Identity (with associated
%%  certtype() 'user').
%%  @see    certtype()
-type certsign() :: self    %%  Self-signed
                 |  ca.     %%  Signed by a CA

-type certinfo() :: {
    certtype(), %%  CA or User
    certname(), %%  CN of this certificate
    certsign(), %%  how this cert is signed
    certpath(), %%  path to the certificate file
    certpath()  %%  path to the private key file
}.

-record(dn, {
    commonName, 
    organizationalUnitName  = ?DEFAULT_ORG,
    organizationName        = ?DEFAULT_OU,
    localityName            = ?DEFAULT_LOC,
    countryName             = ?DEFAULT_COUNTRY,
    emailAddress            = ?DEFAULT_EMAIL,
    default_bits            = ?DEFAULT_BITS
}).

-define(OpenSSLCmd, "openssl").
-define(CaCnfFile,  "ca.cnf").
-define(ReqCnfFile, "req.cnf").
-define(RandFile,   "RAND").
-define(RandFileSz, 1024).

test_cert_dir() ->
    filename:join( rt_config:get(rt_scratch_dir), "certs" ).

priv_cert_dir() ->
    filename:join( rt:priv_dir(), "certs" ).

-define(CA_0, "RootCA").
-define(CA_1, "IntCA").
-define(CA_0_SIGNED, ["site3", "site4"]).
-define(CA_1_SIGNED, ["site1", "site2"]).
-define(SITE_DOMAIN, ".basho.com").

%%  There's a story here, and it ain't pretty ...
-define(HACK_SITES,  ["site5", "site6"]).

create_priv_certs() ->
    TempDir = test_cert_dir(),
    % everyone expects a rand file in this directory
    ok = assure_rnd(TempDir),

    rootCA( TempDir, ?CA_0 ),
    intermediateCA( TempDir, ?CA_1, ?CA_0 ),
    endusers( TempDir, ?CA_0, [N ++ ?SITE_DOMAIN || N <- ?CA_0_SIGNED] ),
    endusers( TempDir, ?CA_1, [N ++ ?SITE_DOMAIN || N <- ?CA_1_SIGNED] ),

    [enduser(TempDir, ?CA_0, SN, req_cnf(SN)) ||
       SN <- [N ++ ?SITE_DOMAIN || N <- ?HACK_SITES]],

    CertDir = priv_cert_dir(),

    SsCertDir = filename:join( CertDir, "selfsigned" ),
    SsCertCAs = filename:join( SsCertDir, "ca" ),
    ok = filelib:ensure_dir( filename:join( SsCertCAs, "foo" ) ),

    SpCertDir = filename:join( CertDir, "special" ),
    SpCertCAs = filename:join( SpCertDir, "ca" ),
    ok = filelib:ensure_dir( filename:join( SpCertCAs, "foo" ) ),

    %%  The CA certs don't follow a pattern, they are what they are
    ok = copy_file( filename:join([TempDir, ?CA_0, "cert.pem"]),
                    filename:join( SsCertCAs, "rootcert.pem" ) ),
    ok = copy_file( filename:join([TempDir, ?CA_1, "cert.pem"]),
                    filename:join( SsCertCAs, "cacert.pem" ) ),
    ok = copy_file( filename:join([TempDir, ?CA_0, "cert.pem"]),
                    filename:join( SpCertCAs, "rootca.pem" ) ),

    %%  The site certs are reasonably pattern based
    [copy_site( TempDir, SsCertDir, SiteName ) ||
                SiteName <- ?CA_0_SIGNED ++ ?CA_1_SIGNED ],
    [copy_site( TempDir, SpCertDir, SiteName ) ||
                SiteName <- ?HACK_SITES ],

    CertDir.

copy_site( Src, Dest, Site ) ->
    Sprefix = filename:join( Src, Site ++ ?SITE_DOMAIN ) ++ "/",
    Dprefix = filename:join( Dest, Site ) ++ "-",
    ok = copy_file( Sprefix ++ "cert.pem", Dprefix ++ "cert.pem" ),
    ok = copy_file( Sprefix ++ "key.pem", Dprefix ++ "key.pem" ),
    ok.

-ifdef(INCLUDE_ALL).
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
-endif. %%  INCLUDE_ALL

rootCA(Root, Name) ->
    create_ca_dir(Root, Name, ca_cnf(Name)),
    DN = #dn{commonName = Name},
    create_self_signed_cert(Root, Name, req_cnf(DN)),
    file:copy( filename:join([Root, Name, "cert.pem"]),
               filename:join([Root, Name, "cacerts.pem"]) ),
    gencrl(Root, Name).

intermediateCA(Root, CA, ParentCA) ->
    create_ca_dir(Root, CA, ca_cnf(CA)),
    CARoot = filename:join([Root, CA]),
    DN = #dn{commonName = CA},
    CnfFile = filename:join([CARoot, ?ReqCnfFile]),
    create_file(CnfFile, req_cnf(DN)),
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

enduser(Root, CA, User) ->
    DN = #dn{commonName = User},
    enduser(Root, CA, User, req_cnf(DN)).

enduser(Root, CA, User, ReqCnf) ->
    UsrRoot = filename:join(Root, User),

    % create_file makes sure the directory exists
    CnfFile = filename:join(UsrRoot, ?ReqCnfFile),
    create_file(CnfFile, ReqCnf),

    KeyFile = filename:join(UsrRoot, "key.pem"),
    ReqFile = filename:join(UsrRoot, "req.pem"),
    create_req(Root, CnfFile, KeyFile, ReqFile),

    CertFileAllUsage = filename:join(UsrRoot, "cert.pem"),
    sign_req(Root, CA, "user_cert", ReqFile, CertFileAllUsage),

    CertFileDigitalSigOnly = filename:join(UsrRoot, "digital_signature_only_cert.pem"),
    sign_req(Root, CA, "user_cert_digital_signature_only", ReqFile, CertFileDigitalSigOnly),

    CACertsFile = filename:join(UsrRoot, "cacerts.pem"),
    copy_file(filename:join([Root, CA, "cacerts.pem"]), CACertsFile),
    ok.

-ifdef(INCLUDE_COLLECT).
collect_certs(Root, CAs, Users) ->
    Bins = lists:foldr(
        fun(CA, Acc) -> 
            File = filename:join([Root, CA, "cert.pem"]),
            {ok, Bin} = file:read_file(File),
            [Bin, "\n" | Acc]
        end, [], CAs),
    lists:foreach(
        fun(User) ->
            File = filename:join([Root, User, "cacerts.pem"]),
            create_file(File, Bins)
        end, Users).
-endif. %%  INCLUDE_COLLECT

revoke(Root, CA, User) ->
    UsrCert = filename:join([Root, User, "cert.pem"]),
    CACnfFile = filename:join([Root, CA, ?CaCnfFile]),
    openssl_cmd([
        "ca",
        "-revoke", UsrCert,
        "-crl_reason", "keyCompromise"
        "-config", CACnfFile],
        [{"ROOTDIR", filename:absname(Root)}] ).

gencrl(Root, CA) ->
    CACnfFile = filename:join([Root, CA, ?CaCnfFile]),
    CACRLFile = filename:join([Root, CA, "crl.pem"]),
    openssl_cmd([
        "ca",
        "-gencrl",
        "-crlhours", "24",
        "-out", CACRLFile,
        "-config", CACnfFile],
        [{"ROOTDIR", filename:absname(Root)}] ).

verify(Root, CA, User) ->
    CAFile = filename:join([Root, User, "cacerts.pem"]),
    CACRLFile = filename:join([Root, CA, "crl.pem"]),
    CertFile = filename:join([Root, User, "cert.pem"]),
    try
        openssl_cmd([
            "verify",
            "-CAfile", CAFile,
            "-CRLfile", CACRLFile, %% this is undocumented, but seems to work
            "-crl_check", CertFile],
            [{"ROOTDIR", filename:absname(Root)}] )
    catch
        error:{openssl, RC, _} when RC > 1 ->
            invalid
    end.

create_self_signed_cert(Root, CAName, Cnf) ->
    CARoot = filename:join([Root, CAName]),
    CnfFile = filename:join([CARoot, ?ReqCnfFile]),
    create_file(CnfFile, Cnf),
    KeyFile = filename:join([CARoot, "private", "key.pem"]), 
    CertFile = filename:join([CARoot, "cert.pem"]), 
    openssl_cmd([
        "req", "-new", "-x509",
        "-config", CnfFile,
        "-keyout", KeyFile,
        "-outform", "PEM",
        "-days", "5478",  % about 15 years
        "-out", CertFile],
        [{"ROOTDIR", filename:absname(Root)}] ).

-ifdef(INCLUDE_ECC_OPS).
create_self_signed_ecc_cert(Root, CAName, Cnf) ->
    CARoot = filename:join([Root, CAName]),
    CnfFile = filename:join([CARoot, ?ReqCnfFile]),
    create_file(CnfFile, Cnf),
    KeyFile = filename:join([CARoot, "private", "key.pem"]), 
    CertFile = filename:join([CARoot, "cert.pem"]),

    create_ecc_key(KeyFile),
    openssl_cmd([
        "req", "-new", "-x509",
        "-key", KeyFile,
        "-outform", "PEM",
        "-out", CertFile,
        "-config", CnfFile],
        [{"ROOTDIR", filename:absname(Root)}] ).

create_ecc_req(Root, CnfFile, KeyFile, ReqFile) ->
    create_ecc_key(KeyFile),
    openssl_cmd([
        "req", "-new",
        "-key", KeyFile,
        "-outform", "PEM",
        "-out", ReqFile,
        "-config", CnfFile],
        [{"ROOTDIR", filename:absname(Root)}] ).

create_ecc_key(KeyFile) ->
    openssl_cmd([
        "ecparam", "-genkey",
        "-name", "secp521r1",
        "-out", KeyFile] ).
-endif. %%  INCLUDE_ECC_OPS

create_req(Root, CnfFile, KeyFile, ReqFile) ->
    openssl_cmd([
        "req", "-new",
        "-config", CnfFile,
        "-outform", "PEM ",
        "-keyout", KeyFile,
        "-out", ReqFile],
        [{"ROOTDIR", filename:absname(Root)}] ).

sign_req(Root, CA, CertType, ReqFile, CertFile) ->
    CACnfFile = filename:join([Root, CA, ?CaCnfFile]),
    openssl_cmd([
        "ca", "-batch",
        "-config", CACnfFile,
        "-extensions", CertType,
        "-in", ReqFile,
        "-out", CertFile],
        [{"ROOTDIR", filename:absname(Root)}] ).

create_ca_dir(Root, CAName, Cnf) ->
    CARoot = filename:join([Root, CAName]),
    ok = filelib:ensure_dir(CARoot),
    ok = create_dir(CARoot),
    create_dirs(CARoot, ["certs", "crl", "newcerts", "private"]),
    assure_rnd(filename:join([CARoot, "private", ?RandFile])),
    create_files(CARoot, [{"serial", "01\n"},
                          {"crlnumber", "01"},
                          {"index.txt", ""},
                          {?CaCnfFile, Cnf}]).

%%
%%  Misc
%%

copy_file(Src, Dest) ->
    case file:copy(Src, Dest) of
        {ok, _} ->
            ok;
        {error, Why} ->
            error({Why, io_lib:format(
                    "failed to copy ~p to ~p", [Src, Dest] )})
    end.

create_dirs(Root, Dirs) ->
    lists:foreach(
        fun(Dir) ->
            create_dir(filename:join(Root, Dir))
        end, Dirs).

create_dir(Dir) ->
    case filelib:is_dir(Dir) of
        true ->
            ok;
        false ->
            ok = filelib:ensure_dir(Dir),
            file:make_dir(Dir)
    end.

create_files(Root, NameContents) ->
    lists:foreach(
        fun({Name, Contents}) ->
            create_file(filename:join([Root, Name]), Contents)
        end, NameContents).

create_file(FilePath, Contents) ->
    ok = filelib:ensure_dir(FilePath),
    case file:write_file(FilePath, Contents) of
        ok ->
            ok;
        {error, Why} ->
            error({Why, "Error writing file: " ++ FilePath})
    end.

assure_rnd(Dir) ->
    RF = filename:join(Dir, ?RandFile),
    case filelib:is_regular(RF) of
        true ->
            ok;
        false ->
            ok = filelib:ensure_dir(RF),
            openssl_cmd(["rand", "-out", RF, integer_to_list(?RandFileSz)])
    end.

-ifdef(INCLUDE_REMOVE_RND).
remove_rnd(Dir) ->
    RF = filename:join(Dir, ?RandFile),
    file:delete(RF).
-endif. %%  INCLUDE_REMOVE_RND

openssl_cmd(CmdArgs) ->
    openssl_cmd(CmdArgs, []).

openssl_cmd(CmdArgs, CmdEnv) ->
    OpenSSL = case os:find_executable("openssl") of
        false ->
            error({enoent, "Unable to locate the OpenSSL executable"});
        Exe ->
            Exe
    end,
    PortName = {spawn_executable, OpenSSL},
    PortOpts = [
        {args, CmdArgs},
        {env, CmdEnv},
        stream, eof, exit_status, stderr_to_stdout, hide],
    Port = open_port( PortName, PortOpts ),
    case eval_openssl( Port ) of
        ok ->
            ok;
        {error, RC, Msg} ->
            What = io_lib:format("Error from ~p : ~p",
                [lists:flatten([OpenSSL] ++ [" " ++ A || A <- CmdArgs]),
                 lists:flatten(Msg)]),
            error({openssl, RC, What})
    end.

eval_openssl(Port) ->
    eval_openssl(Port, []).

eval_openssl(Port, Output) ->
    receive
        {Port, {data, Out}} ->
            eval_openssl(Port, Output ++ Out);
        {Port, eof} ->
            ok
    end,
    receive
        {Port, {exit_status, Status}} ->
            case Status of
                0 ->
                    ok;
                _ ->
                    {error, Status, Output}
            end
        after 0 ->
            ok
    end.

%%
%% Contents of configuration files 
%%

req_cnf(CN) when is_list(CN) ->
    ["#\n"
    "# Configuration for Signing Request.\n"
    "#\n"
    "ROOTDIR                 = $ENV::ROOTDIR\n"
    "\n"
    "[req]\n"
    "input_password          = secret\n"
    "output_password         = secret\n"
    "default_bits            = ", integer_to_list(?DEFAULT_BITS), "\n"
    "RANDFILE                = $ROOTDIR/", ?RandFile, "\n"
    "encrypt_key             = no\n"
    "default_md              = sha1\n"
    "x509_extensions         = ca_ext\n"
    "prompt                  = no\n"
    "distinguished_name      = name\n"
    "\n"
    "[name]\n"
    "commonName              = ", CN, "\n"
    "\n"
    "[ca_ext]\n"
    "basicConstraints        = critical, CA:true\n"
    "keyUsage                = cRLSign, keyCertSign\n"
    "subjectKeyIdentifier    = hash\n"
    "subjectAltName          = email:", ?DEFAULT_EMAIL, "\n"
    "\n"];

req_cnf(DN) when is_record(DN, dn) ->
    ["#\n"
    "# Configuration for Signing Request.\n"
    "#\n"
    "ROOTDIR                 = $ENV::ROOTDIR\n"
    "\n"
    "[req]\n"
    "input_password          = secret\n"
    "output_password         = secret\n"
    "default_bits            = ", integer_to_list(DN#dn.default_bits), "\n"
    "RANDFILE                = $ROOTDIR/", ?RandFile, "\n"
    "encrypt_key             = no\n"
    "default_md              = sha1\n"
    "x509_extensions         = ca_ext\n"
    "prompt                  = no\n"
    "distinguished_name      = name\n"
    "\n"
    "[name]\n"
    "commonName              = ", DN#dn.commonName, "\n"
    "organizationalUnitName  = ", DN#dn.organizationalUnitName, "\n"
    "organizationName        = ", DN#dn.organizationName, "\n" 
    "localityName            = ", DN#dn.localityName, "\n"
    "countryName             = ", DN#dn.countryName, "\n"
    "emailAddress            = ", DN#dn.emailAddress, "\n"
    "\n"
    "[ca_ext]\n"
    "basicConstraints        = critical, CA:true\n"
    "keyUsage                = cRLSign, keyCertSign\n"
    "subjectKeyIdentifier    = hash\n"
    "subjectAltName          = email:copy\n"
    "\n"].

ca_cnf(CA) ->
    ["#\n"
    "# Purpose: Configuration for CAs.\n"
    "#\n"
    "ROOTDIR                 = $ENV::ROOTDIR\n"
    "default_ca              = ca\n"
    "\n"
    "[ca]\n"
    "dir                     = $ROOTDIR/", CA, "\n"
    "certs                   = $dir/certs\n"
    "crl_dir                 = $dir/crl\n"
    "database                = $dir/index.txt\n"
    "new_certs_dir           = $dir/newcerts\n"
    "certificate             = $dir/cert.pem\n"
    "serial                  = $dir/serial\n"
    "crl                     = $dir/crl.pem\n"
    "crlnumber               = $dir/crlnumber\n"
    "private_key             = $dir/private/key.pem\n"
    "RANDFILE                = $dir/private/", ?RandFile, "\n"
    "\n"
    "x509_extensions         = user_cert\n"
    "copy_extensions         = copy\n"
    "crl_extensions          = crl_ext\n"
    "unique_subject          = no\n"
    "default_days            = 3652\n"
    "default_md              = sha1\n"
    "preserve                = no\n"
    "policy                  = policy_match\n"
    "\n"
    "[policy_match]\n"
    "commonName              = supplied\n"
    "organizationalUnitName  = optional\n"
    "organizationName        = optional\n"
    "countryName             = optional\n"
    "localityName            = optional\n"
    "emailAddress            = optional\n"
    "\n"
    "[crl_ext]\n"
    "authorityKeyIdentifier  = keyid:always,issuer:always\n"
    "\n"
    "[user_cert]\n"
    "basicConstraints        = CA:false\n"
    "keyUsage                = nonRepudiation, digitalSignature, keyEncipherment\n"
    "subjectKeyIdentifier    = hash\n"
    "authorityKeyIdentifier  = keyid,issuer:always\n"
    "subjectAltName          = email:copy\n"
    "issuerAltName           = issuer:copy\n"
    "crlDistributionPoints   = @crl_section\n"
    "\n"
    "[crl_section]\n"
    "URI.1=file://$ROOTDIR/",CA,"/crl.pem\n"
    "#URI.2=http://localhost:8000/",CA,"/crl.pem\n"
    "\n"
    "[user_cert_digital_signature_only]\n"
    "basicConstraints        = CA:false\n"
    "keyUsage                = digitalSignature\n"
    "subjectKeyIdentifier    = hash\n"
    "authorityKeyIdentifier  = keyid,issuer:always\n"
    "subjectAltName          = email:copy\n"
    "issuerAltName           = issuer:copy\n"
    "\n"
    "[ca_cert]\n"
    "basicConstraints        = critical,CA:true\n"
    "keyUsage                = cRLSign, keyCertSign\n"
    "subjectKeyIdentifier    = hash\n"
    "authorityKeyIdentifier  = keyid:always,issuer:always\n"
    "subjectAltName          = email:copy\n"
    "issuerAltName           = issuer:copy\n"
    "crlDistributionPoints   = @crl_section\n"
    "\n"].
