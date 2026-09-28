use strict;
use warnings;
use Test::More;
use Test::RedisServer;

my $redis_server;
eval {
    $redis_server = Test::RedisServer->new;
} or plan skip_all => 'redis-server is required to this test';

my %connect_info = $redis_server->connect_info;

use EV;
use EV::Redis;
use lib 't/lib';
use RedisTestHelper qw(get_redis_version);

my ($major, $minor) = get_redis_version($connect_info{sock});

sub settle {
    my ($sec) = @_;
    EV::now_update;
    my $t = EV::timer $sec // 0.2, 0, sub { EV::break };
    EV::run;
}

sub client { EV::Redis->new(path => $connect_info{sock}, on_error => sub {}) }

my $ctl = client();

# the server answers neither CLIENT REPLY OFF nor SKIP, nor the command SKIP covers
for my $mode (qw(off OFF skip)) {
    my $r = client();
    eval { $r->client('reply', $mode, sub {}) };
    like $@, qr/CLIENT REPLY \Q$mode\E is not supported/, "CLIENT REPLY $mode croaks";
    my $got;
    $r->ping(sub { $got = $_[0] });
    settle();
    is $got, 'PONG', "a command after the refused CLIENT REPLY $mode gets its own reply";
    $r->disconnect;
}
{
    my $r = client();
    my $got;
    $r->client('reply', 'on', sub { $got = $_[0] });
    settle();
    is $got, 'OK', 'CLIENT REPLY ON still works';
    $r->disconnect;
}

SKIP: {
    skip 'RESET requires Redis 6.2+', 6 if $major < 6 || ($major == 6 && $minor < 2);

    $ctl->del('ps_list', sub {});
    $ctl->rpush('ps_list', 'a', 'b', 'c', sub {});
    settle();

    my $r = client();
    my @msgs;
    $r->subscribe('ps_chan', sub { push @msgs, $_[0] });
    settle();
    eval { $r->command('reset', sub {}) };
    like $@, qr/RESET is not supported on a subscribed connection/,
        'RESET croaks while subscribed';
    $r->disconnect;

    # RESET waiting behind a SUBSCRIBE
    $r = client();
    $r->max_pending(1);
    $r->ping(sub {});
    $r->subscribe('ps_chan', sub {});
    eval { $r->command('reset', sub {}) };
    like $@, qr/RESET is not supported/, 'RESET croaks with a SUBSCRIBE waiting';
    settle();
    $r->disconnect;

    # not subscribed: RESET is fine, and later replies keep their callbacks
    $r = client();
    my ($reset, $range);
    $r->command('reset', sub { $reset = $_[0] });
    $r->lrange('ps_list', 0, -1, sub { $range = $_[0] });
    settle();
    is $reset, 'RESET', 'RESET on an unsubscribed connection';
    is_deeply $range, [qw(a b c)], 'reply after RESET reaches its callback';
    $r->disconnect;

    # RESP3 with a push seen, then RESET back to RESP2, then subscribe
    $r = client();
    my (@sub, $ping);
    $r->command('hello', 3, sub {});
    $r->subscribe('ps_x', sub {});
    settle();
    $r->unsubscribe('ps_x');
    settle();
    $r->command('reset', sub {});
    settle();
    $r->subscribe('ps_chan2', sub { push @sub, $_[0] });
    $r->ping(sub { $ping = $_[0] });
    settle();
    $ctl->publish('ps_chan2', 'm1', sub {});
    settle();
    is_deeply $ping, ['pong', ''], 'PING after RESET from RESP3 gets its own reply';
    is_deeply [map { $_->[0] } @sub], [qw(subscribe message)],
        'subscription after RESET from RESP3 gets its confirmation and messages';
    $r->disconnect;
}

SKIP: {
    skip 'HELLO requires Redis 6+', 3 if $major < 6;

    # HELLO 3, one push, back to HELLO 2: subscribe traffic must not shift callbacks
    my $r = client();
    my (@sub, $ping1, $ping2);
    $r->command('hello', 3, sub {});
    $r->subscribe('hs_x', sub {});
    settle();
    $r->unsubscribe('hs_x');
    settle();
    $r->command('hello', 2, sub {});
    settle();
    $r->subscribe('hs_chan', sub { push @sub, $_[0] });
    $r->ping(sub { $ping1 = $_[0] });
    settle();
    $ctl->publish('hs_chan', 'm1', sub {});
    settle();
    $r->ping(sub { $ping2 = $_[0] });
    settle();
    is_deeply $ping1, ['pong', ''], 'PING after HELLO 2 gets its own reply';
    is_deeply [map { "$_->[0]:$_->[2]" } @sub], ['subscribe:1', 'message:m1'],
        'subscription after HELLO 2 gets its confirmation and messages';
    is_deeply $ping2, ['pong', ''], 'second PING gets its own reply';
    $r->disconnect;
}

# pub/sub and MONITOR queued inside MULTI would answer QUEUED to the wrong callback
{
    my $r = client();
    my (@got, $sub_err, $mon_err);
    $r->multi(sub { push @got, "multi:$_[0]" });
    $r->subscribe('ms_chan', sub { $sub_err = $_[1] unless defined $_[0] });
    $r->exec(sub { push @got, 'exec:' . (ref $_[0] ? scalar @{$_[0]} : $_[0] // "err $_[1]") });
    $r->ping(sub { push @got, "ping:$_[0]" });
    settle();
    like $sub_err, qr/inside MULTI/, 'SUBSCRIBE inside MULTI is refused through its callback';
    is_deeply \@got, ['multi:OK', 'exec:0', 'ping:PONG'],
        'commands around a refused SUBSCRIBE in MULTI get their own replies';

    $r->multi(sub {});
    settle();
    $r->monitor(sub { $mon_err = $_[1] unless defined $_[0] });
    my $exec;
    $r->exec(sub { $exec = $_[0] });
    settle();
    like $mon_err, qr/inside MULTI/, 'MONITOR inside MULTI is refused';
    is_deeply $exec, [], 'EXEC after a refused MONITOR still works';

    my $sub_ok;
    $r->subscribe('ms_chan', sub { $sub_ok = $_[0] if ref $_[0] });
    settle();
    is $sub_ok->[0], 'subscribe', 'SUBSCRIBE after EXEC works';
    $r->disconnect;
}

# a MONITOR the server refuses must not leave the connection in monitor mode
{
    my $r = client();
    my ($err, @mon, $ping);
    $r->monitor('extra', sub { push @mon, [@_] });
    settle();
    like $mon[0][1], qr/wrong number of arguments/i, 'refused MONITOR reports the error';
    is $r->pending_count, 0, 'nothing pending after a refused MONITOR';
    eval { $r->ping(sub { $ping = $_[0] }); 1 } or $err = $@;
    is $err, undef, 'commands are accepted after a refused MONITOR';
    settle();
    is $ping, 'PONG', 'and get their own replies';
    is scalar @mon, 1, 'the refused MONITOR callback ran once';
    $r->disconnect;
    settle();
    is scalar @mon, 1, 'and is not called again on disconnect';
}

# REPLCONF ACK and GETACK get no reply from a normal client; SYNC and PSYNC
# answer with a replication stream
for my $args ([qw(replconf ack 0)], [qw(replconf getack *)],
              [qw(replconf listening-port 6390 getack *)], [qw(replconf capa eof ack 0)],
              [qw(sync)], [qw(psync ? -1)]) {
    my $r = client();
    eval { $r->command(@$args, sub {}) };
    like $@, qr/(?:REPLCONF ACK and GETACK|$args->[0]) (?:are|is) not supported/i, "@$args croaks";
    my $got;
    $r->ping(sub { $got = $_[0] });
    settle();
    is $got, 'PONG', '... and the next command gets its own reply';
    $r->disconnect;
}

SKIP: {
    skip 'RESET requires Redis 6.2+', 3 if $major < 6 || ($major == 6 && $minor < 2);

    # a RESET that waits while connecting meets the subscription on_connect makes
    my $r;
    my (@msgs, $reset_err, $ping);
    $r = EV::Redis->new(path => $connect_info{sock}, reconnect => 1,
        resume_waiting_on_reconnect => 1, on_error => sub {},
        on_connect => sub { $r->subscribe('rq_chan', sub { push @msgs, $_[0] if ref $_[0] }) });
    $r->command('reset', sub { $reset_err = $_[1] });
    settle();
    $r->ping(sub { $ping = $_[0] });
    $ctl->publish('rq_chan', 'm1', sub {});
    settle();
    like $reset_err, qr/RESET is not supported on a subscribed connection/,
        'a waiting RESET fails once on_connect subscribed';
    is_deeply $ping, ['pong', ''], 'PING after it gets its own reply';
    is_deeply [map { $_->[0] } @msgs], [qw(subscribe message)], 'and the subscription stays';
    $r->reconnect(0);
    $r->disconnect;
}

SKIP: {
    skip 'HELLO requires Redis 6+', 4 if $major < 6;

    # HELLO inside MULTI would switch the protocol inside EXEC's reply, unseen
    $ctl->del('hm_list', sub {});
    $ctl->rpush('hm_list', 'a', 'b', 'c', sub {});
    settle();
    my $r = client();
    my ($hello_err, $range, $ping, $get);
    $r->command('hello', 3, sub {});
    $r->subscribe('hm_chan', sub {});
    settle();
    $r->multi(sub {});
    $r->command('hello', 2, sub { $hello_err = $_[1] });
    $r->exec(sub {});
    settle();
    $r->lrange('hm_list', 0, -1, sub { $range = $_[0] });
    $r->ping(sub { $ping = $_[0] });
    $r->get('hm_nokey', sub { $get = defined $_[0] ? $_[0] : $_[1] // 'nil' });
    settle();
    like $hello_err, qr/HELLO is not supported inside MULTI/, 'HELLO inside MULTI fails';
    is_deeply $range, [qw(a b c)], 'RESP3, subscribed: the next reply reaches its callback';
    is $ping, 'PONG', '... so does the one after';
    is $get, 'nil', '... and the one after that';
    $r->disconnect;
}

# a MULTI the server refuses opens no transaction
{
    my $r = client();
    my ($multi_err, $sub);
    $r->multi('extra', sub { $multi_err = $_[1] });
    settle();
    $r->subscribe('rm_chan', sub { $sub = defined $_[0] ? $_[0][0] : $_[1] });
    settle();
    like $multi_err, qr/wrong number of arguments/i, 'MULTI with an argument is refused';
    is $sub, 'subscribe', 'SUBSCRIBE afterwards is not taken as inside MULTI';
    $r->disconnect;
}

# ... while one pipelined behind it is still open
{
    my $r = client();
    my ($sub_err, $exec, $ping);
    $r->multi('extra', sub {});
    $r->multi(sub {});
    settle();
    $r->subscribe('rm_chan2', sub { $sub_err = $_[1] unless defined $_[0] });
    $r->exec(sub { $exec = $_[0] });
    $r->ping(sub { $ping = $_[0] });
    settle();
    like $sub_err, qr/inside MULTI/, 'SUBSCRIBE inside the open one is refused';
    is_deeply $exec, [], 'EXEC gets its own reply';
    is $ping, 'PONG', '... and so does the next command';
    $r->disconnect;
}

done_testing;
