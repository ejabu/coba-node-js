const Pool = require('pg').Pool
const nestedWrapper = require('./nested').nestedWrapper;

pgConfig = {
    user: 'map_dev@postgrescks',
    host: 'postgrescks.postgres.database.azure.com',
    database: 'mor_view',
    password: 'mapdevelop',
    ssl: true,
}


const pool = new Pool(pgConfig)

query = `
WITH mor AS (
    SELECT
      mor_sk,
      mor_short
    FROM dwh.d_mor
    WHERE
      is_active = TRUE
  ),
  ser AS (
    SELECT
      sbm_sk,
      sbm_short_name,
      mor_sk
    FROM dwh.d_sbm
  ),
  dist AS (
    SELECT
      d.district_sk,
      d.district_name,
      coor.longitude AS lng,
      coor.latitude AS lat
    FROM dwh.d_all_district d
    LEFT JOIN dwh.d_all_district_map coor ON coor.district_sk = d.district_sk
    WHERE
      d.district_isactive = TRUE
  ),
  plant AS (
    SELECT
      dspbu_sk,
      dspbu_no,
      dspbu_name,
      dspbu_contact,
      dspbu_phone,
      dspbu_address,
      dspbu_coordinate_x,
      dspbu_coordinate_y,
      mor_sk,
      prov_sk,
      city_sk
    FROM dwh.d_spbu
  ),
  mtrl AS (
    SELECT
      prod_sk,
      prod_prod_code,
      prod_prod_name
    FROM dwh.d_produk_rfm
  ),
  main AS (
    SELECT
      mor_sk,
      -- DISTRICT
      city_sk,
      -- SER
      sbm_sk,
      -- Plant untuk Tree
      dspbu_sk,
      -- Product
      product_rfm_sk,
      COALESCE(SUM(ytd_quantity), 0) AS y_sale,
      COALESCE(SUM(last_year_ytd_quantity), 0) AS y_targ,
      COALESCE(SUM(mtd_quantity), 0) AS m_sale,
      COALESCE(SUM(last_year_mtd_quantity), 0) AS m_targ
    FROM data_set.ds_map_ytd_mtd_rfm main /*
    WHERE
      city_sk = 29
      AND year = 2019
      AND month_sk = 12 --
      */
      WHERE
      year = 2020
      AND month_sk = 3
    GROUP BY
      main.mor_sk,
      main.sbm_sk,
      main.city_sk,
      main.dspbu_sk,
      main.product_rfm_sk --
     -- LIMIT 100
      /*
    FETCH FIRST
      10 ROWS ONLY --
      */
  ),
  fmain AS (
    SELECT
      'spbu' AS plant_type,
      -- Mor
      main.mor_sk AS mor_id,
      mor.mor_short AS mor_name,
      -- Ser
      main.sbm_sk AS ser_id,
      ser.sbm_short_name AS ser_name,
      -- District
      main.city_sk AS district_id,
      INITCAP(dist.district_name) AS district_name,
      dist.lng AS district_lng,
      dist.lat AS district_lat,
      -- Plant
      main.dspbu_sk AS plant_id,
      CONCAT ('SPBU ', plant.dspbu_no) AS plant_name,
      -- Product
      main.product_rfm_sk AS product_id,
      mtrl.prod_prod_name AS product_name,
      -- YTD
      y_sale,
      y_targ,
      COALESCE(100 * y_sale / NULLIF (y_targ, 0), 0) :: int y_perf,
      -- MTD
      m_sale,
      m_targ,
      COALESCE(100 * m_sale / NULLIF (m_targ, 0), 0) :: int m_perf
    FROM main
    LEFT JOIN mor ON main.mor_sk = mor.mor_sk
    LEFT JOIN ser ON main.sbm_sk = ser.sbm_sk
    LEFT JOIN dist ON main.city_sk = dist.district_sk
    LEFT JOIN plant ON main.dspbu_sk = plant.dspbu_sk
    LEFT JOIN mtrl ON main.product_rfm_sk = mtrl.prod_sk
  ),
  pvt_base AS (
    SELECT
      GROUPING (mor_id, ser_id, district_id, plant_id) node,
      COALESCE (mor_id, 0) as mor_id,
      COALESCE (ser_id, 0) as ser_id,
      COALESCE (district_id, 0) as district_id,
      COALESCE (plant_id, 0) as plant_id,
      COALESCE(SUM(y_sale), 0) AS y_sale,
      COALESCE(SUM(y_targ), 0) AS y_targ,
      COALESCE(SUM(m_sale), 0) AS m_sale,
      COALESCE(SUM(m_targ), 0) AS m_targ
    FROM fmain
    GROUP BY
      ROLLUP (mor_id, ser_id, district_id, plant_id)
    ORDER BY
      node DESC,
      mor_id DESC,
      ser_id DESC,
      district_id DESC,
      plant_id DESC
  ),
  pvt AS (
    select
      array [mor_id, ser_id, district_id, plant_id] pivot_key,
      json_build_object(
        'm_sale',
        m_sale,
        'm_targ',
        m_targ,
        'm_perf',
        COALESCE(100 * m_sale / NULLIF (m_targ, 0), 0) :: int,
        'y_sale',
        y_sale,
        'y_targ',
        y_targ,
        'y_perf',
        COALESCE(100 * y_sale / NULLIF (y_targ, 0), 0) :: int
      ) pivot_value
    FROM pvt_base
  ),
  zmain AS (
    SELECT
      count(*),
      json_agg(fmain),
      (
        SELECT
          JSON_OBJECT_AGG(d.pivot_key :: TEXT, d.pivot_value)
        FROM (
            SELECT
              *
            FROM pvt
          ) d
      ) AS pivot
    FROM fmain
  )
  SELECT
    *
  FROM zmain
`
function executeQuery(req, response) {


    pool.query(query, [], (error, results) => {
        if (error) {
            throw error
        }
        // console.log(results.rows[0]['json_agg'])

        data = {
            'children': [
            ],
        }
        ref_keys = ['mor_id', 'ser_id', 'district_id', 'plant_id'],
        docs = results.rows[0]['json_agg']
        pivot = results.rows[0]['pivot']
        count = results.rows[0]['count']
        result = nestedWrapper(data, ref_keys, docs, pivot)
        vals = {
          count: count,
          result : result,
        }
        response.status(200).json(vals)
    })

}

exports.executeQuery = executeQuery

// exports.index = (req, res) => {
//     res.render('home', {
//         title: 'Home'
//     });
// };
