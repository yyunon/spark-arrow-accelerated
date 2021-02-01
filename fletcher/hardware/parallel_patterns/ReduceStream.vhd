library ieee;
use ieee.std_logic_1164.all;
use ieee.numeric_std.all;

library work;
use work.Stream_pkg.all;
use work.ParallelPatterns_pkg.all;


entity ReduceStream is
  generic (

    -- Width of the stream data vector.
    DATA_WIDTH                  : natural;
    
    -- Width of the stream data vector.
    IN_DIMENSIONALITY           : natural := 1;
    
    -- Bitwidth of the sequence counter
    LENGTH_WIDTH                : natural := 8
    

  );
  port (

    -- Rising-edge sensitive clock.
    clk                         : in  std_logic;

    -- Active-high synchronous reset.
    reset                       : in  std_logic;
    
    -- Init value for the accumulator.
    acc_init_value              : in std_logic_vector(DATA_WIDTH-1 downto 0);

    -- Input stream.
    in_valid                    : in  std_logic;
    in_ready                    : out std_logic;
    in_last                     : in  std_logic_vector(IN_DIMENSIONALITY-1 downto 0);
    
    -- Accumulator output stream.
    acc_out_valid               : out  std_logic;
    acc_out_ready               : in   std_logic;
    acc_out_data                : out  std_logic_vector(DATA_WIDTH-1 downto 0);
    
    -- Accumulator input stream.
    acc_in_valid                : in  std_logic;
    acc_in_ready                : out std_logic;
    acc_in_data                 : in  std_logic_vector(DATA_WIDTH-1 downto 0);
    acc_in_dvalid               : in  std_logic := '1';

    -- Output stream.
    out_valid                   : out std_logic;
    out_ready                   : in  std_logic;
    out_data                    : out std_logic_vector(DATA_WIDTH-1 downto 0);
    out_last                    : out std_logic_vector(IN_DIMENSIONALITY-2 downto 0)
  );
end ReduceStream;

architecture Behavioral of ReduceStream is

  signal count_valid                : std_logic;
  signal count_ready                : std_logic;
  signal count                      : std_logic_vector(LENGTH_WIDTH-1 downto 0);
  signal count_last                 : std_logic;


  signal seq_in_ready               : std_logic;
  signal seq_in_valid               : std_logic;
  signal seq_out_ready              : std_logic;
  signal seq_out_valid              : std_logic;
  signal seq_out_last               : std_logic;
  
  signal acc_s_in_ready             : std_logic;
  signal acc_s_in_valid             : std_logic;
  
  --
  signal out_s_ready                : std_logic;
  signal out_s_valid                : std_logic;
  
  signal acc_out_data_s             : std_logic_vector(DATA_WIDTH-1 downto 0);

begin

-- Count the hanshaked TRANSACTIONS.    
element_counter: StreamElementCounter
    generic map (
      IN_COUNT_MAX              => 1,
      IN_COUNT_WIDTH            => 1,
      OUT_COUNT_MAX             => LENGTH_WIDTH**2,
      OUT_COUNT_WIDTH           => LENGTH_WIDTH
    )
    port map (
      clk                       => clk,
      reset                     => reset,
      in_valid                  => in_valid,
      in_ready                  => in_ready,
      in_dvalid                 => '1',
      in_count                  => "1",
      in_last                   => in_last(0),
      out_valid                 => count_valid,
      out_ready                 => count_ready,
      out_last                  => count_last,
      out_count                 => count
    );
 
 -- StreamSync between the stream sequencer and accumulator.
 -- Also, the data output is driven by the kernel when the last value is processed.
 acc_in_sync: StreamSync
    generic map (
      NUM_INPUTS                => 1,
      NUM_OUTPUTS               => 3
    )
    port map (
      clk                       => clk,
      reset                     => reset,

      in_valid(0)               => acc_in_valid,
      in_ready(0)               => acc_in_ready,


      out_valid(0)              => seq_in_valid,
      out_valid(1)              => acc_s_in_valid,
      out_valid(2)              => out_s_valid,
      out_ready(0)              => seq_in_ready,
      out_ready(1)              => acc_s_in_ready,
      out_ready(2)              => out_s_ready

    );   
    
    
 accumulator: StreamAccumulator
    generic map (
      DATA_WIDTH                => DATA_WIDTH
    )
    port map (
      clk                       => clk,
      reset                     => reset,
      in_valid                  => acc_s_in_valid,
      in_dvalid                 => acc_in_dvalid,
      init_value                => acc_init_value,
      in_ready                  => acc_s_in_ready,
      in_data                   => acc_in_data,
      in_last                   => seq_out_last,
      out_valid                 => acc_out_valid,
      out_ready                 => acc_out_ready,
      out_data                  => acc_out_data_s
    );
 
 
 seq_out_ready <= '1';
 
 sequencer: SequenceStream
    generic map (
      MIN_BUFFER_DEPTH           => 1,
      LENGTH_WIDTH               => LENGTH_WIDTH,
      IN_COUNT_WIDTH             => 2,
      BLOCKING                   => false
    )
    port map (
      clk                       => clk,
      reset                     => reset,
      in_valid                  => seq_in_valid,
      in_ready                  => seq_in_ready,
      in_dvalid                 => '1',
      in_count                  => "01",
      in_length_valid           => count_valid,
      in_length_ready           => count_ready,
      in_length_data            => count,
      out_valid                 => seq_out_valid,
      out_ready                 => seq_out_ready,
      out_last                  => seq_out_last
    );
       
  -- Only valid if the last value is processed in a sequence.
  out_valid <= out_s_valid and seq_out_last;
  
  out_s_ready <= out_ready;
  
  out_data_proc: process(acc_in_data, acc_out_data_s, acc_in_dvalid) is
  begin
    if acc_in_dvalid = '1' then
      out_data <= acc_in_data;
    else
      out_data <= acc_out_data_s;
    end if;
  end process;
  
  -- Outpus last signals are the same in the unaffected dimensions.
  out_last  <= in_last(IN_DIMENSIONALITY-1 downto 1);
  
  acc_out_data <= acc_out_data_s;
  
end Behavioral;
