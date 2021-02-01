library ieee;
use ieee.std_logic_1164.all;
use ieee.numeric_std.all;

library work;
use work.Stream_pkg.all;


-- This unit breaks all combinatorial paths from the input stream to the output
-- stream. All outputs are registers.
--
-- Symbol: --->|--->

entity StreamSliceArray is
  generic (

    -- Width of the stream data vector.
    DATA_WIDTH                  : natural;
    
    -- Numeber of chained slices
    DEPTH                       : natural

  );
  port (

    -- Rising-edge sensitive clock.
    clk                         : in  std_logic;

    -- Active-high synchronous reset.
    reset                       : in  std_logic;

    -- Input stream.
    in_valid                    : in  std_logic;
    in_ready                    : out std_logic;
    in_data                     : in  std_logic_vector(DATA_WIDTH-1 downto 0);

    -- Output stream.
    out_valid                   : out std_logic;
    out_ready                   : in  std_logic;
    out_data                    : out std_logic_vector(DATA_WIDTH-1 downto 0)

  );
end StreamSliceArray;

architecture Behavioral of StreamSliceArray is

  type handshake_signals        is array (0 to DEPTH) of std_logic;
  type data_lanes               is array (0 to DEPTH) of std_logic_vector(DATA_WIDTH-1 downto 0);
  
  signal v: handshake_signals;
  signal r: handshake_signals;
  signal d: data_lanes;

begin
    
  -- Connect ports to the appropriate slices
  v(0)      <= in_valid;
  d(0)      <= in_data;
  in_ready  <= r(0);
  
  out_valid <= v(DEPTH);
  out_data  <= d(DEPTH);
  r(DEPTH)  <= out_ready;
  

  gen_slices : for I in 0 to DEPTH-1 generate
    slice : StreamSlice 
      generic map (
        DATA_WIDTH          => DATA_WIDTH
      )
      port map(
        clk                 => clk,
        reset               => reset,
        in_valid            => v(I),
        in_ready            => r(I),
        in_data             => d(I),
        out_valid           => v(I+1),
        out_ready           => r(I+1),
        out_data            => d(I+1)
      );
  end generate;
  
end Behavioral;
