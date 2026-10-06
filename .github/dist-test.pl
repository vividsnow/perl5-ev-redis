use strict;
use warnings;
use Config;
use Cwd qw(getcwd);
use ExtUtils::MakeMaker;
use ExtUtils::Manifest qw(manicheck);
use File::Temp qw(tempdir);

$| = 1;
my $ssl = $ENV{EV_REDIS_SSL};
die "EV_REDIS_SSL must be 0 or 1\n" unless defined $ssl && $ssl =~ /\A[01]\z/;
my $root = getcwd;
END { chdir $root if defined $root }
my $tmp = tempdir(CLEANUP => 1);
my $dist = 'EV-Redis-' . MM->parse_version('lib/EV/Redis.pm');
my @missing = manicheck();
die "Missing distribution files: @missing\n" if @missing;

sub run {
    print '+ ', join(' ', @_), "\n";
    system(@_) == 0 or die "Command failed ($?): @_\n";
}

run($^X, 'Makefile.PL');
run($Config{make});
run($^X, '-Mblib', '-MEV::Redis', '-e',
    'die "Unexpected TLS build mode\n" unless EV::Redis->has_ssl == $ENV{EV_REDIS_SSL}');
run($Config{make}, 'dist');
run('tar', '-xzf', "$dist.tar.gz", '-C', $tmp);

chdir "$tmp/$dist" or die "chdir distribution: $!\n";
die "Distribution contains Git metadata\n" if -e '.git' || -e 'deps/hiredis/.git';
my $install = "$tmp/install";
run($^X, 'Makefile.PL', "INSTALL_BASE=$install");
run($Config{make});
run($Config{make}, 'test');
run($Config{make}, 'install');

# Load the installed XS outside both build directories, with dependencies from
# the setup action still available through PERL5LIB.
chdir $tmp or die "chdir temporary directory: $!\n";
local $ENV{EV_REDIS_INSTALL_BASE} = $install;
run($^X, '-I', "$install/lib/perl5", '-I', "$install/lib/perl5/$Config{archname}",
    '-MEV::Redis', '-e', q{
        die "Loaded EV::Redis outside the installation\n"
            unless index($INC{'EV/Redis.pm'}, "$ENV{EV_REDIS_INSTALL_BASE}/") == 0;
        die "Unexpected installed TLS build mode\n"
            unless EV::Redis->has_ssl == $ENV{EV_REDIS_SSL};
        EV::Redis->new;
        print "Installed EV::Redis $EV::Redis::VERSION loads\n";
    });
chdir $root or die "chdir source directory: $!\n";
