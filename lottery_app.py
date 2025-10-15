import streamlit as st
import pandas as pd
import pymysql
from datetime import datetime, timedelta
import logging
import io
import time

# 设置页面配置
st.set_page_config(
    page_title="即开票数据查询",
    page_icon="🎫",
    layout="wide",
    initial_sidebar_state="expanded"
)

class LotteryDataExporterStreamlit:
    def __init__(self):
        # 数据库配置
        self.db_config = {
            'host': 'localhost',
            'user': '',
            'password': '',
            'database': 'lottery',
            'charset': 'utf8mb4'
        }
        
        # 列名映射
        self.column_mapping = {
            'region': '兑奖单位',
            'play_method': '方案名称',
            'redeem_site': '兑奖站点',
            'prize_level': '兑奖金额',
            'redeem_time': '兑奖时间',
            'sale_time': '售出时间'
        }
        
        self.table_name = "各奖等中奖明细表"
        
        # 初始化 session state
        self.init_session_state()
    
    def init_session_state(self):
        """初始化 session state"""
        if 'selected_play_methods' not in st.session_state:
            st.session_state.selected_play_methods = []
        if 'prize_conditions' not in st.session_state:
            st.session_state.prize_conditions = {}
        if 'preview_data' not in st.session_state:
            st.session_state.preview_data = None
        if 'play_methods_list' not in st.session_state:
            st.session_state.play_methods_list = []
        if 'regions_list' not in st.session_state:
            st.session_state.regions_list = []
        if 'last_query_success' not in st.session_state:
            st.session_state.last_query_success = False
        if 'log_messages' not in st.session_state:
            st.session_state.log_messages = []
    
    def log_message(self, message):
        """记录日志消息"""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_entry = f"[{timestamp}] {message}"
        st.session_state.log_messages.append(log_entry)
        # 保持日志数量在合理范围内
        if len(st.session_state.log_messages) > 100:
            st.session_state.log_messages = st.session_state.log_messages[-50:]
    
    def setup_ui(self):
        """设置用户界面"""
        st.title("🎫 彩票数据导出工具")
        
        # 侧边栏 - 数据库配置
        with st.sidebar:
            st.header("⚙️ 数据库配置")
            
            self.db_config['host'] = st.text_input(
                "主机", 
                value=self.db_config['host'],
                help="数据库服务器地址"
            )
            self.db_config['user'] = st.text_input(
                "用户名", 
                value=self.db_config['user']
            )
            self.db_config['password'] = st.text_input(
                "密码", 
                value=self.db_config['password'], 
                type="password"
            )
            self.db_config['database'] = st.text_input(
                "数据库", 
                value=self.db_config['database']
            )
            self.table_name = st.text_input(
                "表名", 
                value=self.table_name
            )
            
            col1, col2 = st.columns(2)
            with col1:
                if st.button("🔗 测试连接", use_container_width=True):
                    self.test_connection()
            with col2:
                if st.button("🔄 重置配置", use_container_width=True):
                    self.reset_db_config()
            
            st.markdown("---")
            st.header("📊 快速操作")
            if st.button("🗑️ 清空所有条件", use_container_width=True):
                self.clear_all_conditions()
            
            # 显示系统状态
            st.markdown("---")
            st.header("📈 系统状态")
            st.metric("已选玩法", len(st.session_state.selected_play_methods))
            st.metric("金额条件", len(st.session_state.prize_conditions))
            if st.session_state.preview_data is not None:
                st.metric("查询结果", len(st.session_state.preview_data))
        
        # 主内容区 - 使用标签页组织
        tab1, tab2, tab3, tab4 = st.tabs(["🔍 数据筛选", "📋 数据预览", "💾 数据导出", "📝 操作日志"])
        
        with tab1:
            self.setup_filter_ui()
        
        with tab2:
            self.setup_preview_ui()
        
        with tab3:
            self.setup_export_ui()
        
        with tab4:
            self.setup_log_ui()
    
    def setup_filter_ui(self):
        """设置筛选条件界面"""
        st.header("🔍 数据筛选条件")
        
        # 使用列布局
        col1, col2 = st.columns([1, 1])
        
        with col1:
            st.subheader("🎯 基本条件")
            
            # 兑奖单位选择
            st.write("**🏢 兑奖单位**")
            region_col1, region_col2 = st.columns([3, 1])
            with region_col1:
                selected_region = st.selectbox(
                    "选择兑奖单位",
                    options=[""] + st.session_state.regions_list,
                    key="region_select",
                    label_visibility="collapsed"
                )
            with region_col2:
                if st.button("📥 获取单位", key="fetch_regions", use_container_width=True):
                    self.fetch_regions_from_db()
            
            # 兑奖站点
            redeem_site = st.text_input("🏪 兑奖站点", key="redeem_site")
            
            # 玩法管理
            st.write("**🎮 玩法管理**")
            method_col1, method_col2 = st.columns([3, 1])
            with method_col1:
                selected_method = st.selectbox(
                    "选择玩法",
                    options=[""] + st.session_state.play_methods_list,
                    key="method_select",
                    label_visibility="collapsed"
                )
            with method_col2:
                if st.button("📥 获取玩法", key="fetch_methods", use_container_width=True):
                    self.fetch_play_methods_from_db()
            
            # 添加玩法按钮
            if selected_method and selected_method not in st.session_state.selected_play_methods:
                if st.button("➕ 添加玩法", key="add_method", use_container_width=True):
                    st.session_state.selected_play_methods.append(selected_method)
                    self.log_message(f"已添加玩法: {selected_method}")
                    st.rerun()
            
            # 显示已选玩法列表
            if st.session_state.selected_play_methods:
                st.write("**✅ 已选玩法列表:**")
                for i, method in enumerate(st.session_state.selected_play_methods):
                    method_col1, method_col2 = st.columns([4, 1])
                    method_col1.write(f"• {method}")
                    if method_col2.button("🗑️", key=f"remove_{i}", help=f"移除 {method}"):
                        removed_method = st.session_state.selected_play_methods.pop(i)
                        if removed_method in st.session_state.prize_conditions:
                            del st.session_state.prize_conditions[removed_method]
                        self.log_message(f"已移除玩法: {removed_method}")
                        st.rerun()
                
                if st.button("🗑️ 清空所有玩法", key="clear_all_methods"):
                    st.session_state.selected_play_methods.clear()
                    st.session_state.prize_conditions.clear()
                    self.log_message("已清空所有玩法和金额条件")
                    st.rerun()
            else:
                st.info("ℹ️ 尚未选择任何玩法")
        
        with col2:
            st.subheader("⚡ 高级条件")
            
            # 兑奖金额条件
            st.write("**💰 兑奖金额条件**")
            if st.session_state.selected_play_methods:
                for method in st.session_state.selected_play_methods:
                    amount = st.text_input(
                        f"{method} - 金额条件",
                        value=st.session_state.prize_conditions.get(method, ""),
                        key=f"amount_{method}",
                        placeholder="输入金额，如: 100"
                    )
                    if amount:
                        st.session_state.prize_conditions[method] = amount
                    elif method in st.session_state.prize_conditions and not amount:
                        del st.session_state.prize_conditions[method]
                
                # 快速金额设置
                st.write("**🚀 快速设置**")
                quick_amount_cols = st.columns(5)
                common_amounts = ["5", "10", "50", "100", "500"]
                for i, amount in enumerate(common_amounts):
                    if quick_amount_cols[i].button(f"{amount}元", key=f"quick_{amount}"):
                        for method in st.session_state.selected_play_methods:
                            st.session_state.prize_conditions[method] = amount
                        self.log_message(f"已为所有玩法设置金额: {amount}元")
                        st.rerun()
            else:
                st.info("ℹ️ 请先选择玩法以设置金额条件")
            
            # 时间筛选条件
            st.write("**⏰ 时间范围**")
            
            time_col1, time_col2 = st.columns(2)
            with time_col1:
                use_redeem_time = st.checkbox("启用兑奖时间筛选", key="use_redeem_time")
            with time_col2:
                use_sale_time = st.checkbox("启用销售时间筛选", key="use_sale_time")
            
            if use_redeem_time:
                redeem_col1, redeem_col2 = st.columns(2)
                with redeem_col1:
                    redeem_start = st.date_input(
                        "兑奖开始时间", 
                        value=datetime(2025, 1, 1),
                        key="redeem_start"
                    )
                with redeem_col2:
                    redeem_end = st.date_input(
                        "兑奖结束时间", 
                        value=datetime(2025, 12, 31),
                        key="redeem_end"
                    )
            
            if use_sale_time:
                sale_col1, sale_col2 = st.columns(2)
                with sale_col1:
                    sale_start = st.date_input(
                        "销售开始时间", 
                        value=datetime(2025, 1, 1),
                        key="sale_start"
                    )
                with sale_col2:
                    sale_end = st.date_input(
                        "销售结束时间", 
                        value=datetime(2025, 12, 31),
                        key="sale_end"
                    )
            
            # 快速时间设置
            st.write("**📅 快速时间设置**")
            time_buttons_cols = st.columns(5)
            with time_buttons_cols[0]:
                if st.button("今天", use_container_width=True):
                    self.set_today()
            with time_buttons_cols[1]:
                if st.button("最近7天", use_container_width=True):
                    self.set_last_7_days()
            with time_buttons_cols[2]:
                if st.button("最近30天", use_container_width=True):
                    self.set_last_30_days()
            with time_buttons_cols[3]:
                if st.button("本月", use_container_width=True):
                    self.set_this_month()
            with time_buttons_cols[4]:
                if st.button("上个月", use_container_width=True):
                    self.set_last_month()
        
        # 操作按钮
        st.markdown("---")
        action_col1, action_col2, action_col3, action_col4 = st.columns(4)
        
        with action_col1:
            if st.button("🚀 预览数据", use_container_width=True, type="primary"):
                self.preview_data_func()
        
        with action_col2:
            if st.button("💾 导出数据", use_container_width=True):
                self.export_data()
        
        with action_col3:
            if st.button("🔄 重置条件", use_container_width=True):
                self.clear_filter_conditions()
        
        with action_col4:
            if st.button("📊 查看统计", use_container_width=True):
                self.show_statistics()
    
    def setup_preview_ui(self):
        """设置数据预览界面"""
        st.header("📋 数据预览")
        
        if st.session_state.preview_data is not None:
            if not st.session_state.preview_data.empty:
                st.success(f"✅ 查询到 {len(st.session_state.preview_data)} 条记录")
                
                # 数据显示选项
                col1, col2, col3 = st.columns([2, 1, 1])
                with col1:
                    show_count = st.slider("显示记录数量", 10, 1000, 100, 10)
                with col2:
                    show_all = st.checkbox("显示所有列")
                with col3:
                    if st.button("刷新预览"):
                        st.rerun()
                
                # 显示数据
                display_data = st.session_state.preview_data.head(show_count)
                if not show_all and len(display_data.columns) > 10:
                    # 显示前5列和后5列
                    cols_to_show = list(display_data.columns[:5]) + list(display_data.columns[-5:])
                    display_data = display_data[cols_to_show]
                    st.info("显示前5列和后5列，勾选'显示所有列'查看完整数据")
                
                st.dataframe(display_data, use_container_width=True)
                
                # 数据统计信息
                with st.expander("📊 数据统计信息"):
                    st.write("**数据类型:**")
                    st.write(st.session_state.preview_data.dtypes)
                    
                    st.write("**基本统计:**")
                    numeric_cols = st.session_state.preview_data.select_dtypes(include=['number']).columns
                    if len(numeric_cols) > 0:
                        st.write(st.session_state.preview_data[numeric_cols].describe())
                    else:
                        st.write("没有数值列可统计")
            else:
                st.warning("⚠️ 没有找到符合条件的数据")
                st.info("请调整筛选条件后重新查询")
        else:
            st.info("ℹ️ 请先在「数据筛选」标签页中设置条件并点击「预览数据」")
    
    def setup_export_ui(self):
        """设置数据导出界面"""
        st.header("💾 数据导出")
        
        if st.session_state.preview_data is not None and not st.session_state.preview_data.empty:
            st.success(f"✅ 当前有 {len(st.session_state.preview_data)} 条数据可导出")
            
            # 导出设置
            col1, col2 = st.columns(2)
            
            with col1:
                st.subheader("导出设置")
                export_format = st.radio(
                    "导出格式",
                    ["Excel", "CSV"],
                    horizontal=True
                )
                
                filename = st.text_input(
                    "文件名",
                    value=f"lottery_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
                )
                
                if export_format == "Excel":
                    include_index = st.checkbox("包含行索引", value=False)
                else:
                    encoding = st.selectbox("编码格式", ["utf-8", "gbk", "utf-8-sig"])
            
            with col2:
                st.subheader("导出操作")
                
                if export_format == "Excel":
                    if st.button("📥 下载 Excel 文件", use_container_width=True, type="primary"):
                        self.download_excel(filename, include_index)
                else:
                    if st.button("📥 下载 CSV 文件", use_container_width=True, type="primary"):
                        self.download_csv(filename, encoding)
                
                # 导出统计信息
                st.metric("总记录数", len(st.session_state.preview_data))
                st.metric("数据列数", len(st.session_state.preview_data.columns))
                
                # 预览导出数据
                with st.expander("👀 预览导出数据（前5行）"):
                    st.dataframe(st.session_state.preview_data.head(), use_container_width=True)
        
        else:
            st.warning("⚠️ 没有可导出的数据")
            st.info("请先查询数据后再进行导出操作")
    
    def setup_log_ui(self):
        """设置操作日志界面"""
        st.header("📝 操作日志")
        
        # 日志控制
        col1, col2 = st.columns([3, 1])
        with col1:
            st.write("**最近操作记录:**")
        with col2:
            if st.button("清空日志", use_container_width=True):
                st.session_state.log_messages.clear()
                st.rerun()
        
        # 显示日志
        log_container = st.container()
        with log_container:
            if st.session_state.log_messages:
                for log_entry in reversed(st.session_state.log_messages[-20:]):  # 显示最近20条
                    st.text(log_entry)
            else:
                st.info("暂无日志记录")
    
    def download_excel(self, filename, include_index=False):
        """下载Excel文件"""
        try:
            buffer = io.BytesIO()
            with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
                st.session_state.preview_data.to_excel(
                    writer, 
                    index=include_index, 
                    sheet_name='彩票数据'
                )
            
            st.download_button(
                label="📥 点击下载 Excel 文件",
                data=buffer.getvalue(),
                file_name=f"{filename}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key="excel_download"
            )
            
            self.log_message(f"Excel文件已准备下载: {filename}.xlsx")
            
        except Exception as e:
            st.error(f"导出Excel失败: {e}")
            self.log_message(f"导出Excel失败: {e}")
    
    def download_csv(self, filename, encoding="utf-8"):
        """下载CSV文件"""
        try:
            csv_data = st.session_state.preview_data.to_csv(index=False, encoding=encoding)
            
            st.download_button(
                label="📥 点击下载 CSV 文件",
                data=csv_data,
                file_name=f"{filename}.csv",
                mime="text/csv",
                key="csv_download"
            )
            
            self.log_message(f"CSV文件已准备下载: {filename}.csv")
            
        except Exception as e:
            st.error(f"导出CSV失败: {e}")
            self.log_message(f"导出CSV失败: {e}")
    
    def test_connection(self):
        """测试数据库连接"""
        try:
            self.update_db_config()
            connection = pymysql.connect(**self.db_config)
            connection.close()
            st.sidebar.success("✅ 数据库连接成功！")
            self.log_message("数据库连接测试成功")
        except Exception as e:
            st.sidebar.error(f"❌ 数据库连接失败: {e}")
            self.log_message(f"数据库连接失败: {e}")
    
    def reset_db_config(self):
        """重置数据库配置"""
        self.db_config = {
            'host': 'localhost',
            'user': '',
            'password': '',
            'database': 'lottery',
            'charset': 'utf8mb4'
        }
        self.table_name = "各奖等中奖明细表"
        st.sidebar.success("✅ 数据库配置已重置")
        self.log_message("数据库配置已重置")
        st.rerun()
    
    def fetch_regions_from_db(self):
        """从数据库获取兑奖单位列表"""
        try:
            self.update_db_config()
            connection = pymysql.connect(**self.db_config)
            
            cursor = connection.cursor()
            region_col = self.column_mapping['region']
            cursor.execute(f"SELECT DISTINCT {region_col} FROM {self.table_name} WHERE {region_col} IS NOT NULL AND {region_col} != '' ORDER BY {region_col}")
            results = cursor.fetchall()
            
            st.session_state.regions_list = [result[0] for result in results]
            connection.close()
            
            st.success(f"✅ 从数据库获取到 {len(st.session_state.regions_list)} 个兑奖单位")
            self.log_message(f"从数据库获取到 {len(st.session_state.regions_list)} 个兑奖单位")
            
        except Exception as e:
            st.error(f"❌ 从数据库获取兑奖单位列表失败: {e}")
            self.log_message(f"从数据库获取兑奖单位列表失败: {e}")
    
    def fetch_play_methods_from_db(self):
        """从数据库获取玩法列表"""
        try:
            self.update_db_config()
            connection = pymysql.connect(**self.db_config)
            
            cursor = connection.cursor()
            play_method_col = self.column_mapping['play_method']
            cursor.execute(f"SELECT DISTINCT {play_method_col} FROM {self.table_name} WHERE {play_method_col} IS NOT NULL AND {play_method_col} != ''")
            results = cursor.fetchall()
            
            st.session_state.play_methods_list = [result[0] for result in results]
            connection.close()
            
            st.success(f"✅ 从数据库获取到 {len(st.session_state.play_methods_list)} 种玩法")
            self.log_message(f"从数据库获取到 {len(st.session_state.play_methods_list)} 种玩法")
            
        except Exception as e:
            st.error(f"❌ 从数据库获取玩法列表失败: {e}")
            self.log_message(f"从数据库获取玩法列表失败: {e}")
    
    def update_db_config(self):
        """更新数据库配置（从UI获取当前值）"""
        # 配置已经在UI中实时更新了
        pass
    
    def get_conditions(self):
        """获取筛选条件"""
        conditions = {}
        
        # 兑奖单位条件
        if st.session_state.get('region_select'):
            conditions['region'] = st.session_state.region_select
        
        # 兑奖站点条件
        if st.session_state.get('redeem_site'):
            conditions['redeem_site'] = st.session_state.redeem_site
        
        # 玩法条件
        if st.session_state.selected_play_methods:
            conditions['play_methods'] = st.session_state.selected_play_methods.copy()
        
        # 兑奖金额条件
        if st.session_state.prize_conditions:
            conditions['prize_conditions'] = st.session_state.prize_conditions.copy()
        
        # 时间条件
        if st.session_state.get('use_redeem_time'):
            redeem_start = st.session_state.get('redeem_start')
            redeem_end = st.session_state.get('redeem_end')
            if redeem_start and redeem_end:
                conditions['redeem_start_time'] = redeem_start.strftime('%Y/%m/%d')
                conditions['redeem_end_time'] = redeem_end.strftime('%Y/%m/%d')
        
        if st.session_state.get('use_sale_time'):
            sale_start = st.session_state.get('sale_start')
            sale_end = st.session_state.get('sale_end')
            if sale_start and sale_end:
                conditions['sale_start_time'] = sale_start.strftime('%Y/%m/%d')
                conditions['sale_end_time'] = sale_end.strftime('%Y/%m/%d')
        
        return conditions
    
    def build_query(self, conditions):
        """构建SQL查询语句"""
        base_query = f"SELECT * FROM {self.table_name} WHERE 1=1"
        query_params = []
        
        # 使用正确的列名映射
        region_col = self.column_mapping['region']
        redeem_site_col = self.column_mapping['redeem_site']
        play_method_col = self.column_mapping['play_method']
        prize_level_col = self.column_mapping['prize_level']
        redeem_time_col = self.column_mapping['redeem_time']
        sale_time_col = self.column_mapping['sale_time']
        
        # 兑奖单位条件
        if conditions.get('region'):
            base_query += f" AND {region_col} = %s"
            query_params.append(conditions['region'])
        
        # 兑奖站点条件
        if conditions.get('redeem_site'):
            base_query += f" AND {redeem_site_col} = %s"
            query_params.append(conditions['redeem_site'])
        
        # 玩法条件（多选）
        if conditions.get('play_methods'):
            placeholders = ', '.join(['%s'] * len(conditions['play_methods']))
            base_query += f" AND {play_method_col} IN ({placeholders})"
            query_params.extend(conditions['play_methods'])
        
        # 兑奖金额条件（按票种设置）
        if conditions.get('prize_conditions'):
            prize_conditions = conditions['prize_conditions']
            if prize_conditions:
                prize_conditions_parts = []
                for method, amount in prize_conditions.items():
                    prize_conditions_parts.append(f"({play_method_col} = %s AND {prize_level_col} = %s)")
                    query_params.extend([method, amount])
                
                base_query += " AND (" + " OR ".join(prize_conditions_parts) + ")"
        
        # 兑奖时间条件
        if conditions.get('redeem_start_time') and conditions.get('redeem_end_time'):
            base_query += f" AND DATE({redeem_time_col}) BETWEEN %s AND %s"
            query_params.extend([conditions['redeem_start_time'], conditions['redeem_end_time']])
        
        # 销售时间条件
        if conditions.get('sale_start_time') and conditions.get('sale_end_time'):
            base_query += f" AND DATE({sale_time_col}) BETWEEN %s AND %s"
            query_params.extend([conditions['sale_start_time'], conditions['sale_end_time']])
        
        return base_query, query_params
    
    def preview_data_func(self):
        """预览数据"""
        try:
            self.update_db_config()
            conditions = self.get_conditions()
            
            self.log_message("开始查询数据...")
            self.log_message(f"筛选条件: {conditions}")
            
            # 显示进度
            progress_bar = st.progress(0)
            status_text = st.empty()
            
            status_text.text("正在连接数据库...")
            progress_bar.progress(20)
            
            connection = pymysql.connect(**self.db_config)
            
            status_text.text("构建查询语句...")
            progress_bar.progress(40)
            
            query, params = self.build_query(conditions)
            self.log_message(f"执行查询: {query}")
            self.log_message(f"查询参数: {params}")
            
            status_text.text("执行查询...")
            progress_bar.progress(60)
            
            # 执行查询
            st.session_state.preview_data = pd.read_sql(query, connection, params=params)
            connection.close()
            
            status_text.text("处理查询结果...")
            progress_bar.progress(80)
            
            if st.session_state.preview_data.empty:
                st.session_state.last_query_success = False
                self.log_message("没有找到符合条件的数据")
            else:
                st.session_state.last_query_success = True
                self.log_message(f"查询到 {len(st.session_state.preview_data)} 条记录")
            
            progress_bar.progress(100)
            status_text.text("查询完成！")
            time.sleep(0.5)
            status_text.empty()
            progress_bar.empty()
            
        except Exception as e:
            error_msg = f"查询过程中发生错误: {e}"
            st.error(error_msg)
            self.log_message(error_msg)
            st.session_state.last_query_success = False
    
    def export_data(self):
        """导出数据"""
        if st.session_state.preview_data is None or st.session_state.preview_data.empty:
            st.warning("⚠️ 请先预览数据再进行导出")
            return
        
        if not st.session_state.last_query_success:
            st.warning("⚠️ 上次查询没有成功，请重新预览数据")
            return
        
        # 跳转到导出标签页
        st.success("✅ 数据已准备好，请在「数据导出」标签页中下载")
        self.log_message("数据导出功能已就绪")
    
    def clear_filter_conditions(self):
        """清空筛选条件"""
        # 重置筛选条件
        if 'region_select' in st.session_state:
            st.session_state.region_select = ""
        if 'redeem_site' in st.session_state:
            st.session_state.redeem_site = ""
        
        st.session_state.preview_data = None
        st.session_state.last_query_success = False
        
        st.success("✅ 筛选条件已清空")
        self.log_message("筛选条件已清空")
    
    def clear_all_conditions(self):
        """清空所有条件"""
        st.session_state.selected_play_methods.clear()
        st.session_state.prize_conditions.clear()
        st.session_state.preview_data = None
        st.session_state.last_query_success = False
        
        # 重置UI状态
        if 'region_select' in st.session_state:
            st.session_state.region_select = ""
        if 'redeem_site' in st.session_state:
            st.session_state.redeem_site = ""
        if 'method_select' in st.session_state:
            st.session_state.method_select = ""
        
        st.sidebar.success("✅ 所有条件已清空")
        self.log_message("所有条件已清空")
        st.rerun()
    
    def show_statistics(self):
        """显示统计信息"""
        if st.session_state.preview_data is not None and not st.session_state.preview_data.empty:
            st.subheader("📊 数据统计")
            
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("总记录数", len(st.session_state.preview_data))
            with col2:
                st.metric("数据列数", len(st.session_state.preview_data.columns))
            with col3:
                st.metric("数据类型", f"{len(st.session_state.preview_data.select_dtypes(include=['number']).columns)} 数值列")
            
            # 各玩法数量统计
            if self.column_mapping['play_method'] in st.session_state.preview_data.columns:
                play_method_counts = st.session_state.preview_data[self.column_mapping['play_method']].value_counts()
                st.write("**各玩法记录数量:**")
                st.dataframe(play_method_counts, use_container_width=True)
        else:
            st.warning("暂无数据可统计")
    
    # 时间设置方法
    def set_today(self):
        today = datetime.now().date()
        st.session_state.redeem_start = today
        st.session_state.redeem_end = today
        st.session_state.sale_start = today
        st.session_state.sale_end = today
        self.log_message("时间范围已设置为今天")
        st.rerun()
    
    def set_last_7_days(self):
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=6)
        st.session_state.redeem_start = start_date
        st.session_state.redeem_end = end_date
        st.session_state.sale_start = start_date
        st.session_state.sale_end = end_date
        self.log_message("时间范围已设置为最近7天")
        st.rerun()
    
    def set_last_30_days(self):
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=29)
        st.session_state.redeem_start = start_date
        st.session_state.redeem_end = end_date
        st.session_state.sale_start = start_date
        st.session_state.sale_end = end_date
        self.log_message("时间范围已设置为最近30天")
        st.rerun()
    
    def set_this_month(self):
        today = datetime.now().date()
        start_date = today.replace(day=1)
        if today.month == 12:
            end_date = today.replace(year=today.year + 1, month=1, day=1) - timedelta(days=1)
        else:
            end_date = today.replace(month=today.month + 1, day=1) - timedelta(days=1)
        st.session_state.redeem_start = start_date
        st.session_state.redeem_end = end_date
        st.session_state.sale_start = start_date
        st.session_state.sale_end = end_date
        self.log_message("时间范围已设置为本月")
        st.rerun()
    
    def set_last_month(self):
        today = datetime.now().date()
        if today.month == 1:
            start_date = today.replace(year=today.year - 1, month=12, day=1)
            end_date = today.replace(day=1) - timedelta(days=1)
        else:
            start_date = today.replace(month=today.month - 1, day=1)
            end_date = today.replace(day=1) - timedelta(days=1)
        st.session_state.redeem_start = start_date
        st.session_state.redeem_end = end_date
        st.session_state.sale_start = start_date
        st.session_state.sale_end = end_date
        self.log_message("时间范围已设置为上个月")
        st.rerun()

# 运行应用
def main():
    app = LotteryDataExporterStreamlit()
    app.setup_ui()

if __name__ == "__main__":
    main()